#!/bin/bash
set -euo pipefail

PROFILE=${PROFILE:-management}
INSTANCE_TYPE=c6a.4xlarge
AMI=ami-04eaa218f1349d88b
# Personal key pair: no generic default exists, so it must be provided.
#   KEY_NAME=<your-ec2-key-pair> [KEY_FILE=~/.ssh/<key>.pem] ./launch-ec2.sh
KEY_NAME=${KEY_NAME:-}
KEY_FILE=${KEY_FILE:-~/.ssh/${KEY_NAME}.pem}
SUBNET=subnet-228cc17d
SG=sg-add473b4
VOLUME_SIZE=500
# Track whether the name was given explicitly (env or --name): teardown
# refuses to run against the implicit default name.
NAME_EXPLICIT=false
[ -n "${NAME:-}" ] && NAME_EXPLICIT=true
NAME=${NAME:-clickbench-pg-deltax}
TERMINATE_ONLY=false
REFERENCE_MODE=false

# Parse options
while [[ $# -gt 0 ]]; do
  case "$1" in
    --terminate-only)
      TERMINATE_ONLY=true
      shift
      ;;
    --name)
      if [ $# -lt 2 ] || [ -z "$2" ]; then
        echo "ERROR: --name requires a non-empty value" >&2
        exit 1
      fi
      NAME="$2"
      NAME_EXPLICIT=true
      shift 2
      ;;
    --reference)
      # Adjust the suggested next-step message to point at the correctness
      # reference flow rather than the bench flow.
      REFERENCE_MODE=true
      shift
      ;;
    *)
      echo "Unknown option: $1" >&2
      echo "Usage: $0 [--name <tag-name>] [--reference]" >&2
      echo "       $0 --terminate-only --name <tag-name>" >&2
      exit 1
      ;;
  esac
done

# Teardown must name its target explicitly — never terminate whatever
# happens to hold the default name.
if $TERMINATE_ONLY && ! $NAME_EXPLICIT; then
  echo "ERROR: --terminate-only requires an explicit instance name." >&2
  echo "  $0 --terminate-only --name <tag-name>" >&2
  exit 1
fi

# Launching needs an SSH key pair; teardown (--terminate-only) does not.
if ! $TERMINATE_ONLY && [ -z "$KEY_NAME" ]; then
  echo "ERROR: KEY_NAME is not set." >&2
  echo "Set it to your EC2 key pair, e.g.:" >&2
  echo "  KEY_NAME=mykey KEY_FILE=~/.ssh/mykey.pem $0 --name $NAME-${USER:-$(whoami)}" >&2
  exit 1
fi

# Existing instances with the same name are NEVER torn down implicitly —
# multiple people run benches in this account in parallel (e.g. tsg's
# long-lived clickbench/rtabench/jsonbench boxes). Launching requires a
# free name; explicit teardown requires --terminate-only.
EXISTING=$(aws ec2 describe-instances --profile "$PROFILE" \
  --filters "Name=tag:Name,Values=$NAME" "Name=instance-state-name,Values=running,stopped,pending" \
  --query 'Reservations[*].Instances[*].InstanceId' --output text)

if $TERMINATE_ONLY; then
  if [ -n "$EXISTING" ]; then
    echo "Terminating instance(s) named '$NAME': $EXISTING"
    aws ec2 terminate-instances --profile "$PROFILE" --instance-ids $EXISTING --output text
    aws ec2 wait instance-terminated --profile "$PROFILE" --instance-ids $EXISTING
    echo "Terminated."
  else
    echo "No instance named '$NAME' to terminate."
  fi
  exit 0
fi

if [ -n "$EXISTING" ]; then
  echo "ERROR: instance(s) named '$NAME' already exist: $EXISTING" >&2
  echo "Someone may be using them. Pick a unique name, e.g.:" >&2
  echo "  $0 --name $NAME-${USER:-$(whoami)}" >&2
  echo "Or tear down explicitly first: $0 --terminate-only --name $NAME" >&2
  exit 1
fi

# Enable serial console access (idempotent, account-level setting; only
# needed for the launch flow's serial-console fallback)
echo "Ensuring serial console access is enabled..."
aws ec2 enable-serial-console-access --profile "$PROFILE" --region us-east-1 >/dev/null 2>&1 || true

# User-data script: OOM diagnostics + serial console access
USER_DATA=$(cat <<'USERDATA'
#!/bin/bash
set -x

# Set root password for serial console login
echo 'root:Cb3nch!s3rial#2026' | chpasswd

# Enable root login on serial console
mkdir -p /etc/systemd/system/serial-getty@ttyS0.service.d
cat > /etc/systemd/system/serial-getty@ttyS0.service.d/override.conf <<EOF
[Service]
ExecStart=
ExecStart=-/sbin/agetty --keep-baud 115200,38400,9600 ttyS0 \$TERM
EOF
systemctl daemon-reload
systemctl enable serial-getty@ttyS0.service
systemctl start serial-getty@ttyS0.service

# Configure kernel OOM verbosity
sysctl -w vm.oom_dump_tasks=1
sysctl -w vm.panic_on_oom=0
echo 'vm.oom_dump_tasks=1' >> /etc/sysctl.conf

# Log memory stats every 30s for post-mortem analysis
cat > /usr/local/bin/memlog.sh <<'MEMLOG'
#!/bin/bash
while true; do
  echo "=== $(date -Iseconds) ===" >> /var/log/memlog.txt
  free -m >> /var/log/memlog.txt
  head -5 /proc/meminfo >> /var/log/memlog.txt
  ps aux --sort=-%mem | head -10 >> /var/log/memlog.txt
  sleep 30
done
MEMLOG
chmod +x /usr/local/bin/memlog.sh
nohup /usr/local/bin/memlog.sh &

# Enable GRUB serial console output (for next boot / panic messages)
sed -i 's/GRUB_CMDLINE_LINUX_DEFAULT=.*/GRUB_CMDLINE_LINUX_DEFAULT="console=tty0 console=ttyS0,115200n8"/' /etc/default/grub
update-grub 2>/dev/null || true
USERDATA
)

# Launch new instance
echo "Launching $INSTANCE_TYPE instance..."
INSTANCE_ID=$(aws ec2 run-instances --profile "$PROFILE" \
  --image-id "$AMI" \
  --instance-type "$INSTANCE_TYPE" \
  --key-name "$KEY_NAME" \
  --subnet-id "$SUBNET" \
  --security-group-ids "$SG" \
  --block-device-mappings "[{\"DeviceName\":\"/dev/sda1\",\"Ebs\":{\"VolumeSize\":$VOLUME_SIZE,\"VolumeType\":\"gp2\",\"DeleteOnTermination\":true}}]" \
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$NAME}]" \
  --user-data "$USER_DATA" \
  --query 'Instances[0].InstanceId' --output text)

echo "Instance ID: $INSTANCE_ID"
echo "Waiting for instance to be running..."
aws ec2 wait instance-running --profile "$PROFILE" --instance-ids "$INSTANCE_ID"

IP=$(aws ec2 describe-instances --profile "$PROFILE" \
  --instance-ids "$INSTANCE_ID" \
  --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)

echo "Instance ready: $IP"
echo ""
echo "  ssh -i $KEY_FILE ubuntu@$IP"
echo ""

# Wait for user-data to complete (cloud-init)
echo "Waiting for cloud-init to finish..."
for i in $(seq 1 30); do
  if ssh -i "$KEY_FILE" -o StrictHostKeyChecking=no -o ConnectTimeout=5 "ubuntu@$IP" "cloud-init status --wait" 2>/dev/null; then
    break
  fi
  sleep 5
done

echo ""
echo "Instance ready. Next steps:"
echo ""
echo "  export EC2=$IP"
echo ""
if $REFERENCE_MODE; then
  echo "  make reference EC2=$IP            # vanilla PG, capture query results, commit JSON"
  echo "  make destroy-reference-ec2        # tear down this instance when done"
else
  echo "  make setup EC2=$IP    # full setup: install deps, build, load data"
  echo "  make deploy EC2=$IP   # just recompile + restart"
  echo "  make bench EC2=$IP    # run benchmark"
fi
echo ""
echo "Serial console (if SSH is down):"
echo "  aws ec2-instance-connect send-serial-console-ssh-public-key --profile $PROFILE --instance-id $INSTANCE_ID --serial-port 0 --ssh-public-key file://${KEY_FILE%.pem}.pub --region us-east-1"
echo "  ssh -i $KEY_FILE $INSTANCE_ID.port0@serial-console.ec2-instance-connect.us-east-1.aws"
echo "  Login: root / Cb3nch!s3rial#2026"
