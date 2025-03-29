import discord
import psycopg2
from discord.ext import commands, tasks
from datetime import datetime, timedelta

import os
from dotenv import load_dotenv


# Load .env file
load_dotenv()

DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
SERVER_ID = os.getenv('SERVER_ID')
CHANNEL_ID = os.getenv('CHANNEL_ID')
BOT_TOKEN = os.getenv('BOT_TOKEN')

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT,
)
cursor = conn.cursor()

# Discord bot setup
intents = discord.Intents.default()
intents.messages = True
bot = commands.Bot(command_prefix='!', intents=intents)

# Points for post & QA
POINTS_POST = 3
POINTS_QA = 1

@bot.event
async def on_ready():
    print(f'Logged in as {bot.user}')
    weekly_charge.start()
    monthly_leaderboard.start()

@bot.event
async def on_message(message):
    if message.author == bot.user:
        return

    # Determine channel type and assign points
    cursor.execute("SELECT channel_type FROM Channels WHERE channel_id = %s", (message.channel.id,))
    result = cursor.fetchone()
    if result:
        channel_type = result[0]
        points = POINTS_POST if channel_type == 'post' else POINTS_QA

        # Check for weekly charge
        cursor.execute("SELECT start_date, end_date FROM WeeklyCharges WHERE channel_id = %s", (message.channel.id,))
        charge_period = cursor.fetchone()
        if charge_period:
            start_date, end_date = charge_period
            if start_date <= datetime.now().date() <= end_date:
                points *= 2

        # Update points in the database
        month_year = datetime.now().strftime('%Y-%m-01')
        cursor.execute("""
            INSERT INTO Points (user_id, channel_id, points, month_year)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (user_id, channel_id, month_year)
            DO UPDATE SET points = Points.points + %s
        """, (message.author.id, message.channel.id, points, month_year, points))
        conn.commit()

    await bot.process_commands(message)

# Run every week
@tasks.loop(weeks=1)
async def weekly_charge():
    # Set a weekly charge for a specific channel
    channel_id = CHANNEL_ID
    start_date = datetime.now().date()
    end_date = start_date + timedelta(days=6)
    cursor.execute("""
        INSERT INTO WeeklyCharges (channel_id, start_date, end_date)
        VALUES (%s, %s, %s)
        ON CONFLICT (channel_id, start_date)
        DO NOTHING
    """, (channel_id, start_date, end_date))
    conn.commit()

# Run daily to check if it's the first day of the month
@tasks.loop(days=1)
async def monthly_leaderboard():
    if datetime.now().day == 1:
        cursor.execute("""
            SELECT user_id, channel_id, MAX(points) FROM Points
            WHERE month_year = %s
            GROUP BY channel_id, user_id
        """, (datetime.now().strftime('%Y-%m-01'),))
        leaders = cursor.fetchall()

        for user_id, channel_id, _ in leaders:
            # Award roles
            guild = discord.utils.get(bot.guilds, id=SERVER_ID)
            member = guild.get_member(user_id)
            if member:
                cursor.execute("SELECT category FROM Channels WHERE channel_id = %s", (channel_id,))
                category = cursor.fetchone()[0]
                role_name = f"{category.capitalize()} Master"
                role = discord.utils.get(guild.roles, name=role_name)
                if role:
                    await member.add_roles(role)

# Run the bot
bot.run(BOT_TOKEN)

