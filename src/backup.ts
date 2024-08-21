import { exec, execSync } from "child_process";
import {
  S3Client,
  S3ClientConfig,
  PutObjectCommandInput,
  ListObjectsV2Command,
  DeleteObjectCommand
} from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import { createReadStream, unlink, statSync } from "fs";
import { readdirSync } from "fs";
import path from "path";
import os from "os";

import { env } from "./env.js";
import { createMD5 } from "./util.js";

// Функция для удаления старых файлов в S3
const deleteOldBackups = async (folder: string) => {
  console.log(`Deleting old backups in ${folder}...`);

  const clientOptions: S3ClientConfig = {
    region: env.AWS_S3_REGION,
    forcePathStyle: env.AWS_S3_FORCE_PATH_STYLE
  };

  if (env.AWS_S3_ENDPOINT) {
    clientOptions.endpoint = env.AWS_S3_ENDPOINT;
  }

  const client = new S3Client(clientOptions);

  const listParams = {
    Bucket: env.AWS_S3_BUCKET,
    Prefix: folder
  };

  const { Contents } = await client.send(new ListObjectsV2Command(listParams));

  if (Contents && Contents.length > 0) {
    for (const file of Contents) {
      await client.send(new DeleteObjectCommand({ Bucket: env.AWS_S3_BUCKET, Key: file.Key }));
      console.log(`Deleted ${file.Key}`);
    }
  }
};

// Функция для загрузки бэкапа в S3
const uploadToS3 = async ({ name, path, folder }: { name: string, path: string, folder: string }) => {
  console.log(`Uploading backup to S3 (${folder})...`);

  await deleteOldBackups(folder); // Удаляем старые бэкапы перед загрузкой нового

  const bucket = env.AWS_S3_BUCKET;
  const clientOptions: S3ClientConfig = {
    region: env.AWS_S3_REGION,
    forcePathStyle: env.AWS_S3_FORCE_PATH_STYLE
  };

  if (env.AWS_S3_ENDPOINT) {
    console.log(`Using custom endpoint: ${env.AWS_S3_ENDPOINT}`);
    clientOptions.endpoint = env.AWS_S3_ENDPOINT;
  }

  const key = `${folder}/${name}`;
  const params: PutObjectCommandInput = {
    Bucket: bucket,
    Key: key,
    Body: createReadStream(path),
  };

  if (env.SUPPORT_OBJECT_LOCK) {
    console.log("MD5 hashing file...");
    const md5Hash = await createMD5(path);
    console.log("Done hashing file");
    params.ContentMD5 = Buffer.from(md5Hash, 'hex').toString('base64');
  }

  const client = new S3Client(clientOptions);
  await new Upload({ client, params }).done();

  console.log("Backup uploaded to S3...");
};

// Функция для создания дампа базы данных
const dumpToFile = async (filePath: string) => {
  console.log("Dumping DB to file...");
  await new Promise((resolve, reject) => {
    exec(`pg_dump --dbname=${env.BACKUP_DATABASE_URL} --format=tar ${env.BACKUP_OPTIONS} | gzip > ${filePath}`, (error, stdout, stderr) => {
      if (error) {
        reject({ error: error, stderr: stderr.trimEnd() });
        return;
      }

      const isValidArchive = (execSync(`gzip -cd ${filePath} | head -c1`).length == 1) ? true : false;
      if (!isValidArchive) {
        reject({ error: "Backup archive file is invalid or empty; check for errors above" });
        return;
      }

      if (stderr) {
        console.log({ stderr: stderr.trimEnd() });
      }

      console.log("Backup archive file is valid");
      console.log("Backup filesize:", statSync(filePath).size);

      if (stderr) {
        console.log(`Potential warnings detected; Please ensure the backup file "${path.basename(filePath)}" contains all needed data`);
      }

      resolve(undefined);
    });
  });

  console.log("DB dumped to file...");
};

// Функция для удаления локального файла
const deleteFile = async (path: string) => {
  console.log("Deleting file...");
  await new Promise((resolve, reject) => {
    unlink(path, (err) => {
      if (err) reject({ error: err });
      else resolve(undefined);
    });
  });
};

// Основная функция бэкапа
export const backup = async (type: 'daily' | 'weekly') => {
  console.log("Initiating DB backup...");

  const date = new Date().toISOString();
  const timestamp = date.replace(/[:.]+/g, '-');
  const filename = `${env.BACKUP_FILE_PREFIX}-${timestamp}.tar.gz`;

  const folder = type === 'daily' ? 'daily-backup' : 'weekly-backup';
  const filepath = path.join(os.tmpdir(), filename);

  await dumpToFile(filepath);
  await uploadToS3({ name: filename, path: filepath, folder });
  await deleteFile(filepath);

  console.log("DB backup complete...");
};
