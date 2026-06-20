# Telegram Bot Setup Guide

This guide explains how to create a Telegram bot, connect it to a Telegram channel, obtain the correct `chat_id`, and configure Airflow for `telegram_daily_report.py`.

Official references:

- Telegram bots are created and managed through BotFather. Telegram documents BotFather as the interface for creating and managing bots. 
- `sendMessage`, `sendDocument`, and `getUpdates` are Bot API methods used by this project for sending the Telegram report and testing bot/channel access. 

## 1. Create a Telegram Bot with BotFather

1. Open Telegram.
2. Search for:

```text
@BotFather
```

3. Start a chat with BotFather.
4. Send:

```text
/newbot
```

5. Enter a display name, for example:

```text
Tech News Report Bot
```

6. Enter a bot username. It must end with `bot`, for example:

```text
tech_news_reporter_uni_bot
```

7. BotFather will return a bot token.

Example token format:

```text
1234567890:ABCDEF_xxxxxxxxxxxxxxxxx
```

Keep this token private. Do not commit it to Git.

## 2. Create a Telegram Channel

1. In Telegram, create a new channel.
2. Give it a name, for example:

```text
Tech Report Bot Project
```

3. For easier setup, make the channel public and assign a username.

Example channel link:

```text
https://t.me/tech_news_universe
```

The public channel username/chat ID can then be written as:

```text
@tech_news_universe
```

## 3. Add the Bot to the Channel

1. Open the Telegram channel.
2. Go to channel settings.
3. Open:

```text
Administrators
```

4. Add the bot as an administrator.
5. Give the bot permission to post messages.

Without this permission, the Bot API may return:

```text
Bad Request: chat not found
```

or fail to post into the channel.

## 4. Obtain the Correct Telegram Chat ID

For a public channel, the easiest chat ID is the channel username.

Example:

```text
TELEGRAM_CHAT_ID=@tech_news_universe
```

For a private channel, the chat ID is usually a numeric ID such as:

```text
-1004407292367
```

The numeric ID can be found by sending a test message in the channel and calling `getUpdates`.

## 5. Test the Bot Token and Chat ID

Create this test file:

```text
tests/test_telegram_connection.py
```

```python
"""Test Telegram Bot API connectivity from the Airflow container."""

from __future__ import annotations

import os

import requests


token = os.environ["TELEGRAM_BOT_TOKEN"]
chat_id = os.environ["TELEGRAM_CHAT_ID"]

print("TELEGRAM_CHAT_ID =", chat_id)
print("Token length =", len(token))

me_response = requests.get(
    f"https://api.telegram.org/bot{token}/getMe",
    timeout=30,
)
print("getMe:", me_response.status_code, me_response.text)

message_response = requests.post(
    f"https://api.telegram.org/bot{token}/sendMessage",
    json={
        "chat_id": chat_id,
        "text": "Test message from Airflow DAG04 Telegram setup.",
    },
    timeout=30,
)
print("sendMessage:", message_response.status_code, message_response.text)
```

Copy it into the scheduler container:

```powershell
docker compose cp .\tests\test_telegram_connection.py airflow-scheduler:/tmp/test_telegram_connection.py
```

Run it:

```powershell
docker compose exec airflow-scheduler python /tmp/test_telegram_connection.py
```

Successful output should include:

```text
getMe: 200
sendMessage: 200
```

The channel should receive the test message.

## 6. Configure Telegram Credentials in `.env`

The current project uses environment variables loaded into the Airflow containers.

Add these lines to `.env`:

```env
TELEGRAM_BOT_TOKEN=your_bot_token_from_botfather
TELEGRAM_CHAT_ID=@your_channel_username
```

Example:

```env
TELEGRAM_BOT_TOKEN=1234567890:ABCDEFxxxxxxxxxxxxxxxx
TELEGRAM_CHAT_ID=@tech_news_universe
```

Do not add spaces around `=`.

Correct:

```env
TELEGRAM_CHAT_ID=@tech_news_universe
```

Incorrect:

```env
TELEGRAM_CHAT_ID = @tech_news_universe
```

## 7. Pass Telegram Variables into Docker Compose

In `docker-compose.yml`, under the Airflow environment section, include:

```yaml
TELEGRAM_BOT_TOKEN: ${TELEGRAM_BOT_TOKEN}
TELEGRAM_CHAT_ID: ${TELEGRAM_CHAT_ID}
```

Then recreate the Airflow services:

```powershell
docker compose up -d --force-recreate airflow-api-server airflow-scheduler airflow-dag-processor
```

Verify that the scheduler container can see the values:

```powershell
docker compose exec airflow-scheduler sh -lc 'echo TELEGRAM_CHAT_ID=$TELEGRAM_CHAT_ID && echo TELEGRAM_BOT_TOKEN_LENGTH=${#TELEGRAM_BOT_TOKEN}'
```

Expected output:

```text
TELEGRAM_CHAT_ID=@tech_news_universe
TELEGRAM_BOT_TOKEN_LENGTH=40+
```

## 8. Configure Airflow Variables

If using Airflow Variables instead of `.env`, set them with:

```powershell
docker compose exec airflow-scheduler airflow variables set telegram_bot_token "your_bot_token_from_botfather"
docker compose exec airflow-scheduler airflow variables set telegram_chat_id "@tech_news_universe"
```

For a private channel, use the numeric chat ID:

```powershell
docker compose exec airflow-scheduler airflow variables set telegram_chat_id "-1004407292367"
```

Check that the variables exist:

```powershell
docker compose exec airflow-scheduler airflow variables get telegram_chat_id
```

Do not print the bot token in screenshots or public documentation.

## 9. DAG04 Usage

The Telegram report DAG is:

```text
dags/telegram_daily_report.py
```

It performs this flow:

```text
check_same_day_data_quality_success
    ↓
build_daily_telegram_report
    ↓
send_telegram_report
```

The DAG only sends the report after same-day output from:

```text
data_quality_checks.py
```

has been created.

DAG04 reads from these report-ready tables:

```text
gold.daily_report_top_stories
gold.daily_report_trending_domains
gold.daily_report_top_devto_articles
gold.daily_report_summary_metrics
```

Then it sends:

- Telegram text message
- Attached daily report file

## 10. Troubleshooting

### `Bad Request: chat not found`

Likely causes:

- `TELEGRAM_CHAT_ID` is wrong.
- The value is the bot username instead of the channel username.
- The bot was not added to the channel.
- The bot is not a channel administrator.
- The bot does not have permission to post messages.

### Bot token works but message fails

If `getMe` returns `200` but `sendMessage` returns `400`, the bot token is valid but the channel/chat ID is wrong or inaccessible.

### Environment variable is blank inside Docker

Check Docker Compose config:

```powershell
docker compose config | Select-String "TELEGRAM"
```

Then recreate services:

```powershell
docker compose up -d --force-recreate airflow-api-server airflow-scheduler airflow-dag-processor
```

### PowerShell prints blank variable

Use single quotes so the variable expands inside the container, not in PowerShell:

```powershell
docker compose exec airflow-scheduler sh -lc 'echo TELEGRAM_CHAT_ID=$TELEGRAM_CHAT_ID'
```

not:

```powershell
docker compose exec airflow-scheduler sh -lc "echo TELEGRAM_CHAT_ID=$TELEGRAM_CHAT_ID"
```