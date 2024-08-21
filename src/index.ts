import {CronJob} from "cron";
import {backup} from "./backup.js";
import {env} from "./env.js";

console.log("NodeJS Version: " + process.version);

const tryBackup = async (type: "daily" | "weekly") => {
    try {
        await backup(type);
    } catch (error) {
        console.error("Error while running backup: ", error);
        process.exit(1);
    }
}

if (env.RUN_ON_STARTUP || env.SINGLE_SHOT_MODE) {
    console.log("Running on start backup...");

    await tryBackup('daily');

    if (env.SINGLE_SHOT_MODE) {
        console.log("Database backup complete, exiting...");
        process.exit(0);
    }
}

const dailyJob = new CronJob(env.DAILY_BACKUP_CRON, async () => {
    await tryBackup('daily');
});

const weeklyJob = new CronJob(env.WEEKLY_BACKUP_CRON, async () => {
    await tryBackup('weekly');
});

dailyJob.start();
weeklyJob.start();


console.log("Backups cron scheduled...");