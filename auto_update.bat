@echo off
:: ── 100X Algo Dashboard — Auto Update ─────────────────────────────────────
:: Fetches fresh data from Delta Exchange and pushes to GitHub Pages
:: Run this manually or schedule via Windows Task Scheduler

cd /d "C:\Users\win10\Desktop\DELTA TRADE JOURNAL"

:: Set API keys
set NATARAJ_API_KEY=DGAGU0iblaj6pa5STowM5tUlVWs55m
set NATARAJ_API_SECRET=GjepupvulGiHmEy1lXMIznyUhub0j6GQwL91UgqkejAKym6iSOIn8yCBn2LX
set SHALINI_API_KEY=AMYXgPjy7P8F9VJ9C8mIOJNJnVZUiC
set SHALINI_API_SECRET=3an2OKlsW7sXkkDQFvWqKHxBGtpI2A52uGXXEYt8Te3qJZAS1nNEP2h9ICb0
set MOM_API_KEY=x7WzTZ3iZmwBiZc7KBe76haC8gOLVu
set MOM_API_SECRET=3WRsURDn2eBLJ5DSinhW8jyyN7erfszjn0LOFxVaRQCx3lVcnxVajVQCOvEP

:: Fetch data
echo [%date% %time%] Fetching dashboard data...
python delta_fetcher.py

if %errorlevel% neq 0 (
    echo [ERROR] Python fetch failed. Check delta_fetcher.py
    exit /b 1
)

:: Push to GitHub
echo [%date% %time%] Pushing to GitHub...
git add data/
git diff --cached --quiet && (
    echo [INFO] No data changes — skipping push
) || (
    git commit -m "Auto update %date% %time%"
    git push
    echo [%date% %time%] Done. Dashboard updated.
)
