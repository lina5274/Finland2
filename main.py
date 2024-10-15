import asyncio
import aiohttp
import aiomysql
import pandas as pd
import json
import os

CLIENT_ID = os.environ.get('LINKEDIN_CLIENT_ID')
CLIENT_SECRET = os.environ.get('LINKEDIN_CLIENT_SECRET')
REDIRECT_URI = 'http://localhost:8080'


class LinkedInBot:
    def __init__(self):
        self.access_token = None
        self.refresh_token = None
        self.pool = None

    async def authenticate(self):
        auth_url = 'https://www.linkedin.com/oauth/v2/authorization'
        params = {
            'response_type': 'code',
            'client_id': CLIENT_ID,
            'redirect_uri': REDIRECT_URI,
            'state': 'your_state_value_here'
        }

        print(
            f"Please visit this URL to authorize the app:\n{auth_url}?{'&'.join([f'{k}={v}' for k, v in params.items()])}")
        auth_code = input("Enter the authorization code here: ")

        token_url = 'https://www.linkedin.com/oauth/v2/accessToken'
        payload = {
            'grant_type': 'authorization_code',
            'code': auth_code,
            'redirect_uri': REDIRECT_URI,
            'client_id': CLIENT_ID,
            'client_secret': CLIENT_SECRET
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(token_url, data=payload) as response:
                tokens = await response.json()

        self.access_token = tokens['access_token']
        self.refresh_token = tokens['refresh_token']

    async def refresh_access_token(self):
        refresh_url = 'https://www.linkedin.com/oauth/v2/accessToken'
        payload = {
            'grant_type': 'refresh_token',
            'refresh_token': self.refresh_token,
            'client_id': CLIENT_ID,
            'client_secret': CLIENT_SECRET
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(refresh_url, data=payload) as response:
                tokens = await response.json()

        self.access_token = tokens['access_token']
        self.refresh_token = tokens['refresh_token']

    async def connect_to_db(self):
        self.pool = await aiomysql.create_pool(
            host='your_host',
            port=3306,
            db='your_database',
            user='your_username',
            password='your_password'
        )

    async def execute_query(self, query, params=None):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, params)
                return await cur.fetchall()

    async def search_jobs(self):
        await self.connect_to_db()

        # Проверяем, есть ли уже результаты для последнего запроса
        cached_result = await self.get_cached_results()
        if not cached_result.empty:
            return cached_result

        # Если результатов нет, выполняем запрос к API
        url = "https://api.linkedin.com/v2/jobs"
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json',
            'Accept-Language': 'en_US'
        }

        params = {
            'fields': 'id,title,description,location,company,createdBy,createdDate,jobType,experienceLevel,experienceQualification,skills,visibility,postingStatus,postingDate,postingReason,postAndApplyEnabled,postAndApplyLink,postAndApplyText,postAndApplyImageUrls,postAndApplyButtonText,postAndApplyButtonUrl,postAndApplyDescription,contactInfo,emailAddress,phoneNumbers,name'
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params=params) as response:
                jobs = await response.json()

        job_data = []
        for job in jobs['elements']:
            email = job['contactInfo'].get('emailAddress', '')
            phone = next((pn['number'] for pn in job['contactInfo']['phoneNumbers']), '')
            name = f"{job['createdBy'].get('firstName', '')} {job['createdBy'].get('lastName', '')}"

            job_data.append({
                'Job Title': job['title'],
                'Company Name': job['company'].get('name', ''),
                'Location': job['location'],
                'Email': email,
                'Phone': phone,
                'Responsible Person': name
            })

        insert_query = """
        INSERT INTO job_search_results (results)
        VALUES (%s)
        """
        await self.execute_query(insert_query, (json.dumps(job_data)))

        return pd.DataFrame(job_data)

    async def get_cached_results(self):
        await self.connect_to_db()
        query = """
        SELECT results FROM job_search_results ORDER BY id DESC LIMIT 1
        """
        result = await self.execute_query(query)

        return pd.DataFrame(json.loads(result[0][0])) if result else pd.DataFrame()


async def main():
    bot = LinkedInBot()
    await bot.authenticate()

    # Поиск вакансий
    vacancies_df = await bot.search_jobs()

    print(vacancies_df)


# Запуск асинхронного кода
if __name__ == "__main__":
    asyncio.run(main())
