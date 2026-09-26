import time
import requests

class TelegramClient():
    def __init__(self, logger, bot_token, chat_id, topic_id=None, timeout = 10):
        """
        Initialize the Telegram client with API and connection settings.
        Args:
            logger: Logger instance used to record Telegram request events.
            bot_token: Telegram bot token used to authenticate API requests.
            chat_id: Telegram chat ID where messages will be sent.
            topic_id: Optional Telegram topic/thread ID.
            timeout: Request timeout in seconds.
        """
        self.chat_id = chat_id
        self.topic_id = topic_id
        self.timeout = timeout
        telegram_api_url = "https://api.telegram.org/bot{token}/sendMessage"
        self.url = telegram_api_url.format(token=bot_token)
        self.session = requests.Session()
        self.logger = logger


    def send_message(self, text, max_retries = 3):
        """
        Send an HTML-formatted message to Telegram with retry support.
        Args:
            text: Message content to be sent to Telegram.
            max_retries: Maximum number of attempts if the request fails.
        Returns:
            bool: True if the message is sent successfully, otherwise False.
        """
        payload = {
            "chat_id": self.chat_id,
            "message_thread_id": int(self.topic_id) if self.topic_id else None,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }

        for attempt in range(1, max_retries + 1):
            try:
                response = self.session.post(
                    self.url,
                    json=payload,
                    timeout=self.timeout,
                )

                if response.ok:
                    self.logger.info("Telegram message sent successfully")
                    return True

                self.logger.error(f"Telegram API error: status={response.status_code} | response={response.text}")

            except requests.RequestException as exc:
                self.logger.error(f"Telegram request failed: attempt={attempt}/{max_retries} error={exc}")

            except Exception as error:
                self.logger.error(f"Unexpected error while sending Telegram message: {error}")
            
            if attempt < max_retries:
                time.sleep(2 ** (attempt - 1))

        return False


    def close(self):
        """
        Close the HTTP session and release its resources.
        """
        self.session.close()