const functions = require('@google-cloud/functions-framework');
const { Storage } = require('@google-cloud/storage');
const { spawn } = require('child_process');
const fs = require('fs');
const { v4: uuidv4 } = require('uuid');

const storage = new Storage();
const cacheBucket = storage.bucket('ranktop-v-cache');
const outputBucket = storage.bucket('ranktop-v');

const getRankColor = (idx) => ['#FFD700', '#C0C0C0', '#CD7F32', 'white', 'white'][idx] || 'white';

class ProgressTracker {
  constructor(res) {
    this.res = res;
    this.totalSteps = 6;
    this.currentStep = 0;
  }

  update(message, progress = null) {
    this.currentStep++;
    const calculatedProgress = progress ?? Math.round((this.currentStep / this.totalSteps) * 100);
    const data = { step: this.currentStep, totalSteps: this.totalSteps, progress: calculatedProgress, message, timestamp: Date.now() };
    this.res.write(`data: ${JSON.stringify(data)}\n\n`);
  }

  complete(videoUrl) {
    const data = { step: this.totalSteps, totalSteps: this.totalSteps, progress: 100, message: 'Complete!', videoUrl, complete: true, timestamp: Date.now() };
    this.res.write(`data: ${JSON.stringify(data)}\n\n`);
    this.res.end();
  }

  error(errorMessage) {
    this.res.write(`data: ${JSON.stringify({ error: errorMessage, timestamp: Date.now() })}\n\n`);
    this.res.end();
  }
}

functions.http('processVideos', async (req, res) => {
  res.set({ 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST', 'Access-Control-Allow-Headers': 'Content-Type' });
  if (req.method === 'OPTIONS') return res.status(204).send('');
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const { action } = req.body;

  if (action === 'getUploadUrls') {
    try {
      const { videoCount, sessionId } = req.body;
      if (!videoCount || !sessionId) return res.status(400).json({ error: 'Missing videoCount or sessionId' });

      const uploadUrls = [];
      for (let i = 0; i < videoCount; i++) {
        const fileName = `${sessionId}/video_${i}.mp4`;
        const [url] = await cacheBucket.file(fileName).getSignedUrl({
          version: 'v4',
          action: 'write',
          expires: Date.now() + 15 * 60 * 1000,
          contentType: 'video/mp4',
        });
        uploadUrls.push({ index: i, url });
      }

      return res.json({ uploadUrls, sessionId });
    } catch (error) {
      return res.status(500).json({ error: 'Failed to generate upload URLs' });
    }
  }

  res.set({ 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', 'Connection': 'keep-alive' });
  const tracker = new ProgressTracker(res);

  try {
    const { sessionId, title, ranks, videoOrder, filePaths } = req.body;
    if (!sessionId || !title || !ranks || !filePaths) throw new Error('Missing required data');

    const parsedRanks = typeof ranks === 'string' ? JSON.parse(ranks) : ranks;
    const parsedVideoOrder = typeof videoOrder === 'string' ? JSON.parse(videoOrder) : videoOrder;

    tracker.update('Downloading videos from storage...', 10);
    const localFiles = await downloadVideos(filePaths);

    tracker.update('Processing videos with overlays...', 30);
    const processedVideos = await processVideos(localFiles, title, parsedRanks, parsedVideoOrder);

    tracker.update('Stitching videos together...', 70);
    const finalVideo = await concatenateVideos(processedVideos);

    tracker.update('Uploading final video...', 90);
    const publicUrl = await uploadToGCS(finalVideo, `${title.replace(/[^a-zA-Z0-9]/g, '_')}_${Date.now()}.mp4`);

    tracker.update('Cleaning up...', 95);
    await cleanup(sessionId, [...localFiles, ...processedVideos, finalVideo]);

    tracker.complete(publicUrl);
  } catch (error) {
    tracker.error(error.message);
    await cleanup(req.body.sessionId || 'unknown').catch(() => {});
  }
});

async function downloadVideos(filePaths) {
  const localFiles = [];
  for (let i = 0; i < filePaths.length; i++) {
    const localPath = `/tmp/input_${i}_${uuidv4()}.mp4`;
    await cacheBucket.file(filePaths[i]).download({ destination: localPath });
    localFiles.push(localPath);
  }
  return localFiles;
}

async function processVideos(files, title, ranks, videoOrder) {
  const processedVideos = [];
  const sortedFiles = videoOrder.map(idx => files[idx]).filter(Boolean);

  for (let i = 0; i < sortedFiles.length; i++) {
    const outputPath = `/tmp/processed_${i}_${uuidv4()}.mp4`;
    await addTextOverlay(sortedFiles[i], outputPath, title, ranks, i + 1);
    processedVideos.push(outputPath);
  }

  return processedVideos;
}

function addTextOverlay(inputPath, outputPath, title, ranks, ranksToShow) {
  return new Promise((resolve, reject) => {
    const titleFontSize = 140;
    const rankFontSize = 80;
    const titleY = 60;
    const rankStartY = titleY + titleFontSize + 40;
    const rankSpacing = rankFontSize + 30;
    const fontParam = 'fontfile=/usr/share/fonts/truetype/DejaVuSans.ttf';

    let filter = 'scale=1080:1920:force_original_aspect_ratio=decrease,pad=1080:1920:(ow-iw)/2:(oh-ih)/2';
    filter += `,drawtext=${fontParam}:fontsize=${titleFontSize}:text='${title.replace(/[':]/g, '\\$&')}':fontcolor=white:box=0:borderw=8:bordercolor=black:x=(w-text_w)/2:y=${titleY}`;

    for (let i = 0; i < ranksToShow; i++) {
      const rankIdx = ranks.length - ranksToShow + i;
      const y = rankStartY + i * rankSpacing;
      const rankNumber = rankIdx + 1;
      const rankText = ranks[rankIdx];
      const rankColor = getRankColor(rankIdx);

      filter += `,drawtext=${fontParam}:fontsize=${rankFontSize}:text='${rankNumber}.':fontcolor=${rankColor}:box=0:borderw=6:bordercolor=black:x=80:y=${y}`;
      filter += `,drawtext=${fontParam}:fontsize=${rankFontSize}:text=' ${rankText.replace(/[':]/g, '\\$&')}':fontcolor=white:box=0:borderw=6:bordercolor=black:x=160:y=${y}`;
    }

    const ffmpeg = spawn('ffmpeg', [
      '-i', inputPath,
      '-vf', filter,
      '-c:v', 'libx264',
      '-c:a', 'aac',
      '-preset', 'ultrafast',
      '-crf', '23',
      '-movflags', '+faststart',
      '-y', outputPath,
    ]);

    let stderrOutput = '';

    ffmpeg.stderr.on('data', (data) => {
      stderrOutput += data.toString();
    });

    ffmpeg.on('close', (code) => {
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(`FFmpeg overlay failed with code: ${code}\n${stderrOutput}`));
      }
    });

    ffmpeg.on('error', (error) => {
      reject(new Error(`FFmpeg spawn error: ${error.message}`));
    });
  });
}

function concatenateVideos(inputPaths) {
  return new Promise((resolve, reject) => {
    const outputPath = `/tmp/final_${uuidv4()}.mp4`;
    const concatFile = `/tmp/concat_${uuidv4()}.txt`;
    fs.writeFileSync(concatFile, inputPaths.map(p => `file '${p}'`).join('\n'));

    const ffmpeg = spawn('ffmpeg', [
      '-f', 'concat',
      '-safe', '0',
      '-i', concatFile,
      '-c', 'copy',
      '-movflags', '+faststart',
      '-y', outputPath
    ]);

    let stderrOutput = '';

    ffmpeg.stderr.on('data', (data) => {
      stderrOutput += data.toString();
    });

    ffmpeg.on('close', (code) => {
      fs.unlinkSync(concatFile);
      if (code === 0) {
        resolve(outputPath);
      } else {
        reject(new Error(`Concat failed with code ${code}\n${stderrOutput}`));
      }
    });

    ffmpeg.on('error', reject);
  });
}

async function uploadToGCS(filePath, fileName) {
  const destination = `processed/${fileName}`;
  await outputBucket.upload(filePath, {
    destination,
    metadata: { cacheControl: 'public, max-age=31536000' },
  });
  await outputBucket.file(destination).makePublic();
  return `https://storage.googleapis.com/ranktop-v/${destination}`;
}

async function cleanup(sessionId, localFiles = []) {
  localFiles.forEach(filePath => {
    try {
      if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
    } catch {}
  });

  try {
    const tempFiles = fs.readdirSync('/tmp').filter(f => f.includes('input_') || f.includes('processed_') || f.includes('final_'));
    tempFiles.forEach(f => {
      try { fs.unlinkSync(`/tmp/${f}`); } catch {}
    });
  } catch {}

  if (sessionId && sessionId !== 'unknown') {
    try {
      const [files] = await cacheBucket.getFiles({ prefix: `${sessionId}/` });
      await Promise.all(files.map(file => file.delete().catch(() => {})));
    } catch {}
  }
}
