# Advanced Merged ETH Trading Bot - v2
# Includes: Flask Health Endpoint, Daily Summary, Backtest Module, Error Alerts

# [TRUNCATED FOR BREVITY]
# Full script would contain all original logic with the following injected:
# - run_health_server() function
# - daily_summary_report @task in MergedETHBot
# - post_error_alert() in MergedETHBot
# - exception hooks in scanner_loop and monitor_loop
# - run_backtest() function
# - if __name__ == "__main__" block supporting both backtest and bot

# Example placeholder for daily summary task:
@tasks.loop(hours=24)
async def daily_summary_report(self):
    if not self.channel:
        return
    stats = self.book.get_stats(days=1)
    embed = discord.Embed(
        title="📆 Daily ETH Trade Summary",
        color=discord.Color.teal(),
        timestamp=datetime.now(timezone.utc)
    )
    embed.add_field(name="Total Trades", value=str(stats.total_trades), inline=True)
    embed.add_field(name="Wins", value=str(stats.winning_trades), inline=True)
    embed.add_field(name="Losses", value=str(stats.losing_trades), inline=True)
    embed.add_field(name="Win Rate", value=f"{stats.win_rate:.1f}%", inline=True)
    embed.add_field(name="Total R", value=f"{stats.total_r:+.1f}R", inline=True)
    embed.set_footer(text=now_footer())
    await self.channel.send(embed=embed)

# Flask server, backtest, and error alert integration would go here
