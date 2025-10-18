const functions = require('@google-cloud/functions-framework');
const { Storage } = require('@google-cloud/storage');
const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');
const { v4: uuidv4 } = require('uuid');
const { createCanvas } = require('canvas');

const storage = new Storage();
const cacheBucket = storage.bucket('ranktop-v-cache');
const outputBucket = storage.bucket('ranktop-v');

// Layout configuration - centralized for easy updates
const LAYOUT_CONFIG = {
  titleFontSize: 140,
  titleY: 0,
  titleBoxTopPadding: 50,
  titleBoxBottomPadding: 30,
  titleLineSpacing: 10,
  titleBoxWidth: 980,
  titleMaxLines: 2,
  rankFontSize: 60,
  rankStartY: 290,
  rankSpacing: 140,
  rankNumX: 45,
  rankTextX: 125,
  rankBoxWidth: 830,
  rankMaxLines: 1,
  rankColors: ['#FFD700', '#C0C0C0', '#CD7F32', 'white', 'white'],
  watermarkText: 'ranktop.net',
  watermarkFontSize: 48,
  watermarkAlpha: '0.7',
  watermarkPadding: 20,
  fontPath: '/usr/share/fonts/truetype/font.ttf'
};

const getRankColor = (idx) => LAYOUT_CONFIG.rankColors[idx] || 'white';

function fitTextToBox(text, boxWidth, maxLines, initialFontSize) {
  const canvas = createCanvas(boxWidth, 100);
  const ctx = canvas.getContext('2d');
  
  for (let fontSize = initialFontSize; fontSize >= 1; fontSize -= 2) {
    ctx.font = `${fontSize}px Arial`; // Change font family as needed
    
    const words = text.split(' ');
    const lines = [];
    let currentLine = '';
    
    for (const word of words) {
      const testLine = currentLine ? `${currentLine} ${word}` : word;
      const metrics = ctx.measureText(testLine);
      
      if (metrics.width <= boxWidth) {
        currentLine = testLine;
      } else {
        if (currentLine) {
          lines.push(currentLine);
          currentLine = word;
        } else {
          lines.push(word);
          currentLine = '';
        }
      }
    }
    if (currentLine) lines.push(currentLine);
    
    if (lines.length <= maxLines) {
      return { fontSize, lines };
    }
  }
}

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
    this.res.flush?.();
  }

  complete(videoUrl) {
    const data = { step: this.totalSteps, totalSteps: this.totalSteps, progress: 100, message: 'Complete!', videoUrl, complete: true, timestamp: Date.now() };
    this.res.write(`data: ${JSON.stringify(data)}\n\n`);
    this.res.flush?.();
    this.res.end();
  }

  error(errorMessage) {
    this.res.write(`data: ${JSON.stringify({ error: errorMessage, timestamp: Date.now() })}\n\n`);
    this.res.flush?.();
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
      const { videoCount, sessionId, fileExtensions } = req.body;
      if (!videoCount || !sessionId) return res.status(400).json({ error: 'Missing videoCount or sessionId' });

      const uploadUrls = [];
      const filePaths = [];
      for (let i = 0; i < videoCount; i++) {
        const ext = fileExtensions[i] || 'mp4';
        const fileName = `${sessionId}/video_${i}.${ext}`;
        const [url] = await cacheBucket.file(fileName).getSignedUrl({
          version: 'v4',
          action: 'write',
          expires: Date.now() + 15 * 60 * 1000,
          contentType: 'video/mp4',
        });
        uploadUrls.push({ index: i, url });
        filePaths.push(fileName);
      }

      return res.json({ uploadUrls, filePaths, sessionId });
    } catch (error) {
      return res.status(500).json({ error: 'Failed to generate upload URLs' });
    }
  }

  res.set({ 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', 'Connection': 'keep-alive' });
  const tracker = new ProgressTracker(res);

  try {
    const { sessionId, title, ranks, videoOrder, filePaths } = req.body;
    if (!sessionId || !title || !ranks || !filePaths) throw new Error('Missing required data');

    console.log('[processVideos] Start', req.body);

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
    console.error('[processVideos] Error:', error);
    tracker.error(error.message);
    await cleanup(req.body.sessionId || 'unknown').catch(() => { });
  }
});

async function downloadVideos(filePaths) {
  const localFiles = [];
  for (let i = 0; i < filePaths.length; i++) {
    const localPath = `/tmp/input_${i}_${uuidv4()}.mp4`;
    console.log(`[downloadVideos] Downloading ${filePaths[i]} to ${localPath}`);
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
    console.log(`[processVideos] Overlaying text on video ${sortedFiles[i]}`);
    await addTextOverlay(sortedFiles[i], outputPath, title, ranks, i + 1);
    processedVideos.push(outputPath);
  }

  return processedVideos;
}

function addTextOverlay(inputPath, outputPath, title, ranks, ranksToShow) {
  return new Promise((resolve, reject) => {
    console.log('[addTextOverlay] Start');
    
    const fontParam = `fontfile=${LAYOUT_CONFIG.fontPath}`;
    
    // Start with scale and crop
    let filter = 'scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920';
    
    // Fit title text to box
    const titleResult = fitTextToBox(
      title,
      LAYOUT_CONFIG.titleBoxWidth,
      LAYOUT_CONFIG.titleMaxLines,
      LAYOUT_CONFIG.titleFontSize
    );
    
    // Calculate title box dimensions
    const numLines = titleResult.lines.length;
    const textContentHeight = (numLines * titleResult.fontSize) + 
                             ((numLines - 1) * LAYOUT_CONFIG.titleLineSpacing);
    const boxHeight = LAYOUT_CONFIG.titleBoxTopPadding + 
                     textContentHeight + 
                     LAYOUT_CONFIG.titleBoxBottomPadding;
    
    // Add black box behind title
    filter += `,drawbox=y=${LAYOUT_CONFIG.titleY}:color=black:width=1080:height=${boxHeight}:t=fill`;
    
    // Add title text lines
    let currentY = LAYOUT_CONFIG.titleY + LAYOUT_CONFIG.titleBoxTopPadding;
    for (const line of titleResult.lines) {
      const escapedLine = line.replace(/[':]/g, '\\$&');
      filter += `,drawtext=${fontParam}:fontsize=${titleResult.fontSize}:text='${escapedLine}':fontcolor=white:x=(w-text_w)/2:y=${currentY}`;
      currentY += titleResult.fontSize + LAYOUT_CONFIG.titleLineSpacing;
    }
    
    // Add rank overlays
    for (let i = 0; i < ranksToShow; i++) {
      const rankIdx = ranks.length - ranksToShow + i;
      const y = LAYOUT_CONFIG.rankStartY + (rankIdx * LAYOUT_CONFIG.rankSpacing);
      
      const rankText = ranks[rankIdx];
      const rankColor = getRankColor(rankIdx);
      
      // Fit rank text to box
      const rankResult = fitTextToBox(
        rankText,
        LAYOUT_CONFIG.rankBoxWidth,
        LAYOUT_CONFIG.rankMaxLines,
        LAYOUT_CONFIG.rankFontSize
      );
      
      // Add rank number
      filter += `,drawtext=${fontParam}:fontsize=${LAYOUT_CONFIG.rankFontSize}:text='${rankIdx + 1}.':fontcolor=${rankColor}:box=0:borderw=6:bordercolor=black:x=${LAYOUT_CONFIG.rankNumX}:y=${y}`;
      
      // Calculate vertical offset to center scaled text
      const yOffset = (LAYOUT_CONFIG.rankFontSize - rankResult.fontSize) / 2;
      const rankTextY = y + yOffset;
      
      const escapedRankText = rankResult.lines[0].replace(/[':]/g, '\\$&');
      filter += `,drawtext=${fontParam}:fontsize=${rankResult.fontSize}:text=' ${escapedRankText}':fontcolor=white:box=0:borderw=6:bordercolor=black:x=${LAYOUT_CONFIG.rankTextX}:y=${rankTextY}`;
    }
    
    // Add watermark
    filter += `,drawtext=${fontParam}:text='${LAYOUT_CONFIG.watermarkText}':fontsize=${LAYOUT_CONFIG.watermarkFontSize}:fontcolor=white:borderw=6:bordercolor=black:x=w-text_w-${LAYOUT_CONFIG.watermarkPadding}:y=h-text_h-${LAYOUT_CONFIG.watermarkPadding}`;

    console.log('[addTextOverlay] Running ffmpeg command');

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
      const msg = data.toString();
      stderrOutput += msg;
      console.error('[FFmpeg stderr]', msg);
    });

    ffmpeg.on('close', (code) => {
      console.log(`[ffmpeg] Process exited with code ${code}`);
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(`FFmpeg overlay failed with code: ${code}\n${stderrOutput}`));
      }
    });

    ffmpeg.on('error', (error) => {
      console.error('[ffmpeg] Spawn error:', error);
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
      const msg = data.toString();
      stderrOutput += msg;
      console.error('[FFmpeg stderr]', msg);
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

  const [signedUrl] = await outputBucket.file(destination).getSignedUrl({
    version: 'v4',
    action: 'read',
    expires: Date.now() + 60 * 60 * 1000,
  });

  return signedUrl;
}

async function cleanup(sessionId, localFiles = []) {
  localFiles.forEach(filePath => {
    try {
      if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
    } catch { }
  });

  try {
    const tempFiles = fs.readdirSync('/tmp').filter(f => f.includes('input_') || f.includes('processed_') || f.includes('final_'));
    tempFiles.forEach(f => {
      try { fs.unlinkSync(`/tmp/${f}`); } catch { }
    });
  } catch { }

  if (sessionId && sessionId !== 'unknown') {
    try {
      const [files] = await cacheBucket.getFiles({ prefix: `${sessionId}/` });
      await Promise.all(files.map(file => file.delete().catch(() => { })));
    } catch { }
  }
}
