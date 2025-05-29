import ssl
import os
from slack_sdk import WebClient
from datetime import datetime
from config.config import SLACK_TOKEN
# from slack_sdk.http import urllib3

class SlackLogger:
    def __init__(self, default_channel="trading-log"):
        # SSL 검증 비활성화
        context = ssl._create_unverified_context()
        self.client = WebClient(token=SLACK_TOKEN, ssl=context)
        self.default_channel = default_channel
        
    def send_log(self, level, message, error=None, context=None, channel=None):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # 로그 레벨에 따른 이모지 설정
        level_emoji = {
            "INFO": ":information_source:",
            "WARNING": ":warning:",
            "ERROR": ":red_circle:",
            "CRITICAL": ":rotating_light:"
        }.get(level.upper(), ":question:")

        # 기본 블록 구성
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{level_emoji} {level.upper()} Alert"
                }
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*When:*\n{timestamp}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Environment:*\n{os.environ.get('ENV', 'development')}"
                    }
                ]
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Message:*\n```{message}```"
                }
            }
        ]

        # 에러 정보가 있는 경우 추가
        if error:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Error Details:*\n```{str(error)}```"
                }
            })

        # 컨텍스트 정보가 있는 경우 추가
        if context:
            context_text = "\n".join([f"*{k}:* {v}" for k, v in context.items()])
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Additional Context:*\n{context_text}"
                }
            })

        # 구분선 추가
        blocks.append({"type": "divider"})

        # 메시지 전송
        try:
            response = self.client.chat_postMessage(
                channel=channel or self.default_channel,
                blocks=blocks,
                text=f"{level.upper()}: {message}"  # 폴백 텍스트
            )
            return response
        except Exception as e:
            print(f"Failed to send Slack message: {str(e)}")
            return None

# 사용 예시
if __name__ == "__main__":
    slack_logger = SlackLogger("xoxb-your-token")
    
    # 기본 로그
    slack_logger.send_log(
        level="INFO",
        message="User authentication successful",
        context={"user_id": "U123456", "ip": "192.168.1.1"}
    )
    
    # 에러 로그
    try:
        raise ValueError("Database connection failed")
    except Exception as e:
        slack_logger.send_log(
            level="ERROR",
            message="Failed to connect to database",
            error=e,
            context={
                "database": "users_db",
                "server": "prod-db-01",
                "retry_count": 3
            }
        )