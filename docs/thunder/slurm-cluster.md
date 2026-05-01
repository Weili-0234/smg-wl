# SLURM cluster access (current dev session)

## Active allocation

- **JobID**: `30385`
- **Node**: `research-secure-31` (FQDN: `research-secure-31.cloud.together.ai`)
- **GPUs**: 8× NVIDIA H100 80GB HBM3
- **User**: `hkang`

## Access pattern (the ONLY way)

**Cannot SSH to compute nodes.** All execution on compute node MUST go through:

```bash
srun --jobid=30385 --overlap --gpus=0 bash -c '<command>'
```

- `--jobid=30385`: targets the running allocation
- `--overlap`: allows running alongside existing job steps (necessary for multiple parallel `srun` invocations)
- `--gpus=0`: do NOT request GPU resources for control-plane / management commands (servers that need GPUs reserve their own via different srun calls)
- The command runs on `research-secure-31`; output streams back to wherever the `srun` was invoked (login node).

## Practical patterns

### Run a one-off command
```bash
srun --jobid=30385 --overlap --gpus=0 bash -c 'hostname && uptime'
```

### Start a long-running server (background, persistent across srun calls)
```bash
srun --jobid=30385 --overlap --gpus=2 bash -c 'nohup vllm serve Qwen/Qwen3-0.6B --port 8001 > /tmp/vllm-1.log 2>&1 &'
```

### Reaching a service running on the compute node
**Conservative rule**: do NOT assume the login node can reach `research-secure-31:<PORT>` directly. Network reachability from login → compute node is unverified and the sandbox blocks probing. Treat compute-node services as **only reachable from inside the compute node**. That means clients (pytest, manual `curl`) must ALSO run via `srun`:

```bash
srun --jobid=30385 --overlap --gpus=0 bash -c 'curl -sS http://localhost:8001/v1/models'
srun --jobid=30385 --overlap --gpus=0 bash -c 'cd /home/hkang/wl/smg-wl && uv run pytest e2e_test/thunder/'
```

This rule means everything — backends, sidecars, SMG binary, pytest — runs on the compute node. The login node is for editing files and `cargo build` (CPU-only build).

### Discover which jobids you have running
```bash
squeue -u hkang
```

## Filesystem

`/home/hkang/` is shared between login node and compute nodes (NFS or similar). Edits made on the login node are visible on the compute node immediately. Builds (`cargo build`, `uv sync`) can run on the login node and the resulting binaries are then executed on the compute node via `srun`.
