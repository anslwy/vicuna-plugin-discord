# Vicuna Discord Bridge Plugin

Talk to any Vicuna Work session from Discord DMs — with pairing, live progress edits, interactive question/permission buttons, attachments, `/model`-style switching, and voice memo transcription via Vicuna's Whisper.

## Install in Vicuna (v0.0.53+)

1. Open **Plugins** in Vicuna and install **Discord** from **Explore** (or enter `anslwy/vicuna-plugin-discord` under **Installed**).
2. Right-click any **Work** session → **Discord**.
3. Create a bot in the [Discord Developer Portal](https://discord.com/developers/applications) → **Bot** → **Reset Token** and paste the token into Vicuna.
4. Click the generated **Invite** link to add the bot to a server you're in, then DM `!pair <code>` to your bot.

No privileged intents (like Message Content Intent) are required because the bridge uses Discord DMs (`DIRECT_MESSAGES` intent).

## Commands (in Discord DM)

- `!status` — show whether the session is working/idle and its harness/model
- `!model` — pick harness and model with buttons (or `!model claude haiku`)
- `!send_now` — interrupt the running turn and send queued messages now
- `!stop` — stop the running turn
- `!help` — list commands

## Build Locally

```bash
cargo build --release
```
