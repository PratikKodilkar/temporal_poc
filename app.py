import openmeteo_requests
import requests_cache
import pandas as pd
import base64
import logging
import sendgrid
import os
from retry_requests import retry
from sqlalchemy import create_engine
from sendgrid.helpers.mail import Mail, Attachment, FileContent, FileName, FileType, Disposition, ContentId

class WeatherDataProcessor:
    def __init__(self):
        # Initialize caching and retry sessions for HTTP requests
        self.cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
        self.retry_session = retry(self.cache_session, retries=5, backoff_factor=0.2)
        # Initialize OpenMeteo client with the retry session
        self.openmeteo = openmeteo_requests.Client(session=self.retry_session)
        # Configure logging
        logging.basicConfig(filename='weather_processor.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def fetch_weather_data(self, latitude, longitude):
        try:
            # API endpoint and parameters for weather data
            url = "https://api.open-meteo.com/v1/forecast"
            params = {
                "latitude": latitude,
                "longitude": longitude,
                "daily": ["weather_code", "temperature_2m_max", "temperature_2m_min", "sunrise", "precipitation_sum", "rain_sum"],
                "timezone": "auto",
                "forecast_days": 14
            }
            # Fetch weather data from OpenMeteo API
            responses = self.openmeteo.weather_api(url, params=params)
            response = responses[0]

            # Process the daily received weather data.
            daily = response.Daily()
            daily_weather_code = daily.Variables(0).ValuesAsNumpy()
            daily_temperature_2m_max = daily.Variables(1).ValuesAsNumpy()
            daily_temperature_2m_min = daily.Variables(2).ValuesAsNumpy()
            daily_sunrise = daily.Variables(3).ValuesAsNumpy()
            daily_precipitation_sum = daily.Variables(4).ValuesAsNumpy()
            daily_rain_sum = daily.Variables(5).ValuesAsNumpy()

            daily_data = {"date": pd.date_range(
                start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
                end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
                freq = pd.Timedelta(seconds = daily.Interval()),
                inclusive = "left"
            )}
            daily_data["weather_code"] = daily_weather_code
            daily_data["temperature_2m_max"] = daily_temperature_2m_max
            daily_data["temperature_2m_min"] = daily_temperature_2m_min
            daily_data["sunrise"] = daily_sunrise
            daily_data["precipitation_sum"] = daily_precipitation_sum
            daily_data["rain_sum"] = daily_rain_sum

            # Create DataFrame from processed data
            weather_df = pd.DataFrame(data = daily_data)
            return weather_df
        except Exception as e:
            # Log error if fetching weather data fails
            logging.error(f"Error occurred while fetching weather data: {str(e)}")
    
    def store_into_database(self, dataframe,table_name='weather_forecast'):
        try:
            # Create SQLAlchemy engine for SQLite database
            engine = create_engine('sqlite:///weather.db', echo=False)
            dataframe.to_sql(table_name, con=engine, if_exists='replace')
            # Log success message
            logging.info(f"Data successfully stored into '{table_name}' table.")
        except Exception as e:
            # Log error if storing data into database fails
            logging.error(f"Error occurred while storing data into database: {str(e)}")

    def send_email(self, content, weather_dataframe, receiver_email):
        try:
            # HTML content for the email
            html_content = f'''
            <html>
            <head>
                <style>
                    body {{
                        font-family: Arial, sans-serif;
                        font-size: 14px;
                    }}
                    .greeting {{
                        font-size: 18px;
                        font-weight: bold;
                    }}
                    .content {{
                        margin-top: 10px;
                    }}
                    .signature {{
                        font-weight: bold;
                    }}
                </style>
            </head>
            <body>
                <p class="greeting">Hi Team,</p>
                <div class="content">
                    <p>{content}</p>
                </div>
                <p>Thanks,</p>
                <p class="signature">Pratik Kodilkar</p>
            </body>
            </html>
            '''
            # Construct email message
            message = Mail(
                from_email=os.environ.get('EMAIL_USER'),
                to_emails=[receiver_email],
                subject='Weather Report',
                html_content=html_content
            )

            # Encode DataFrame to CSV and attach it to the email
            base64_csv = base64.b64encode(weather_dataframe.to_csv(index=False).encode()).decode()
            message.attachment = Attachment(
                FileContent(base64_csv),
                FileName('weather_report.csv'),
                FileType('text/csv'),
                Disposition('attachment'),
                ContentId('dataframe')
            )

            # Initialize SendGrid client and send the email
            sg = sendgrid.SendGridAPIClient(api_key=os.environ.get('SENDGRID_API_KEY'))
            response = sg.send(message)
            # Log success or failure of sending email
            if response.status_code == 202:
                logging.info("Email sent successfully.")
            else:
                logging.error(f"Failed to send email. Status code: {response.status_code}")
        except Exception as e:
            # Log error if sending email fails
            logging.error(f"Error occurred while sending email: {str(e)}")

if __name__ == "__main__":
    processor = WeatherDataProcessor()
    latitude = 40.7143
    longitude = -74.006
    weather_dataframe = processor.fetch_weather_data(latitude, longitude)
    content = "Following weather_report.csv file contains weather forecast of 2 weeks"
    receiver_email = input("Enter receiver's email address: ")
    processor.store_into_database(weather_dataframe)
    processor.send_email(content, weather_dataframe, receiver_email)
