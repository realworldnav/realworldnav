# Docker Sandbox for Claude Code

## First-Time Setup (One Time Only)

### 1. Make sure Docker Desktop is running

Open Docker Desktop and wait until it says "Engine running".

### 2. Build the container

```bash
cd C:\Users\charl\Documents\GitHub\realworldnav
docker compose build
```

This takes a few minutes. It installs Python, Node, your pip dependencies, and Claude Code CLI.

### 3. Start the container and log in

```bash
docker compose run --rm claude_sandbox
```

You're now inside the container. Log in to Claude:

```bash
claude login
```

Follow the browser prompts to authenticate with your Anthropic account.

### 4. Verify it works

```bash
claude --version
```

You're done with setup. Exit the container:

```bash
exit
```

---

## Every Time You Want to Use It

### Step 1: Open a terminal in the project folder

```bash
cd C:\Users\charl\Documents\GitHub\realworldnav
```

### Step 2: Start the sandbox

```bash
docker compose run --rm claude_sandbox
```

### Step 3: Run Claude with full autonomy

```bash
claude --dangerously-skip-permissions
```

That's it. Claude can now read/write your project files and run any commands, all inside the sandbox.

### Step 4: When you're done

Type `/exit` to quit Claude, then:

```bash
exit
```

This destroys the container (`--rm` flag). Your project files are safe because they live on your host machine.

---

## Quick Reference (Copy-Paste)

```bash
cd C:\Users\charl\Documents\GitHub\realworldnav
docker compose run --rm claude_sandbox
claude --dangerously-skip-permissions
```

---

## If Something Goes Wrong

### Claude messed up my files

```bash
git checkout .
```

Or if you want to keep the changes but review them first:

```bash
git diff
```

### Container won't start

Rebuild it:

```bash
docker compose build --no-cache
```

### "claude: command not found"

The npm install may have failed during build. Rebuild:

```bash
docker compose build --no-cache
```

### Need to re-login

Auth tokens expire. Just run `claude login` again inside the container.

### Added new pip dependencies

If you changed `requirements.txt`, rebuild:

```bash
docker compose build
```
