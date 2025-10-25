const functions = require('@google-cloud/functions-framework');
const { Storage } = require('@google-cloud/storage');
const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');
const { v4: uuidv4 } = require('uuid');
const { createCanvas, registerFont } = require('canvas');

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
  fontPath: '/usr/share/fonts/truetype/font.ttf',
  textOutlineWidth: 12
};

const getRankColor = (idx) => LAYOUT_CONFIG.rankColors[idx] || 'white';

// Register font once at module level
if (!fs.existsSync(LAYOUT_CONFIG.fontPath)) {
  throw new Error(`Font file not found at ${LAYOUT_CONFIG.fontPath}`);
}
registerFont(LAYOUT_CONFIG.fontPath, { family: 'CustomFont' });

function fitTextToBox(text, boxWidth, maxLines, initialFontSize) {
  const canvas = createCanvas(boxWidth, 100);
  const ctx = canvas.getContext('2d');
  
  for (let fontSize = initialFontSize; fontSize >= 1; fontSize -= 2) {
    ctx.font = `${fontSize}px CustomFont`;
    
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

function createTextOverlayImage(title, ranks, ranksToShow) {
  const width = 1080;
  const height = 1920;
  const canvas = createCanvas(width, height);
  const ctx = canvas.getContext('2d');

  // Make background transparent
  ctx.clearRect(0, 0, width, height);

  // Set font baseline for consistent positioning
  ctx.textBaseline = 'top';
  ctx.textAlign = 'left';

  // === TITLE SECTION ===
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

  // Draw black box behind title - centered vertically in available space
  const titleBoxY = LAYOUT_CONFIG.titleY;
  ctx.fillStyle = 'black';
  ctx.fillRect(0, titleBoxY, width, boxHeight);

  // Draw title text lines - centered within the box
  ctx.fillStyle = 'white';
  ctx.font = `${titleResult.fontSize}px CustomFont`;
  
  // Calculate starting Y to center text within the box
  let currentY = titleBoxY + ((boxHeight - textContentHeight) / 2);
  
  for (const line of titleResult.lines) {
    const textWidth = ctx.measureText(line).width;
    const x = (width - textWidth) / 2; // Center horizontally
    ctx.fillText(line, x, currentY);
    currentY += titleResult.fontSize + LAYOUT_CONFIG.titleLineSpacing;
  }

  // === RANK SECTION ===
  // Show ranks progressively: first video shows last rank, second shows last 2, etc.
  const startRankIdx = ranks.length - ranksToShow;
  
  for (let i = 0; i < ranksToShow; i++) {
    const rankIdx = startRankIdx + i;
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

    // Draw rank number with black outline
    ctx.font = `${LAYOUT_CONFIG.rankFontSize}px CustomFont`;
    const rankNumText = `${rankIdx + 1}.`;
    
    // Black outline for rank number
    ctx.strokeStyle = 'black';
    ctx.lineWidth = LAYOUT_CONFIG.textOutlineWidth;
    ctx.strokeText(rankNumText, LAYOUT_CONFIG.rankNumX, y);
    
    // Rank number fill
    ctx.fillStyle = rankColor;
    ctx.fillText(rankNumText, LAYOUT_CONFIG.rankNumX, y);

    // Draw rank text with proper vertical centering
    ctx.font = `${rankResult.fontSize}px CustomFont`;
    
    // Calculate vertical offset to center text with rank number
    // Both use textBaseline='top', so we center based on font sizes
    const rankNumHeight = LAYOUT_CONFIG.rankFontSize;
    const rankTextHeight = rankResult.fontSize;
    
    // Center the smaller text within the space of the larger one
    const verticalOffset = (rankNumHeight - rankTextHeight) / 2;
    const rankTextY = y + verticalOffset;
    
    // Black outline for rank text
    ctx.strokeStyle = 'black';
    ctx.lineWidth = LAYOUT_CONFIG.textOutlineWidth;
    ctx.strokeText(rankResult.lines[0], LAYOUT_CONFIG.rankTextX, rankTextY);
    
    // Rank text fill
    ctx.fillStyle = 'white';
    ctx.fillText(rankResult.lines[0], LAYOUT_CONFIG.rankTextX, rankTextY);
  }

  // === WATERMARK ===
  ctx.font = `${LAYOUT_CONFIG.watermarkFontSize}px CustomFont`;
  const watermarkMetrics = ctx.measureText(LAYOUT_CONFIG.watermarkText);
  const watermarkX = width - watermarkMetrics.width - LAYOUT_CONFIG.watermarkPadding;
  const watermarkY = height - LAYOUT_CONFIG.watermarkFontSize - LAYOUT_CONFIG.watermarkPadding;
  
  // Black outline
  ctx.strokeStyle = 'black';
  ctx.lineWidth = LAYOUT_CONFIG.textOutlineWidth;
  ctx.strokeText(LAYOUT_CONFIG.watermarkText, watermarkX, watermarkY);
  
  // White fill
  ctx.fillStyle = 'white';
  ctx.fillText(LAYOUT_CONFIG.watermarkText, watermarkX, watermarkY);

  return canvas;
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
      const { videoCount, sessionId, fileTypes } = req.body;
      if (!videoCount || !sessionId) return res.status(400).json({ error: 'Missing videoCount or sessionId' });

      const uploadUrls = [];
      const filePaths = [];
      for (let i = 0; i < videoCount; i++) {
        const contentType = fileTypes?.[i] || 'video/mp4';
        const ext = contentType.split('/')[1] || 'mp4';
        const fileName = `${sessionId}/video_${i}.${ext}`;
        const [url] = await cacheBucket.file(fileName).getSignedUrl({
          version: 'v4',
          action: 'write',
          expires: Date.now() + 15 * 60 * 1000,
          contentType: contentType,
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
    const ext = path.extname(filePaths[i]) || '.mp4';
    const localPath = `/tmp/input_${i}_${uuidv4()}${ext}`;
    console.log(`[downloadVideos] Downloading ${filePaths[i]} to ${localPath}`);
    await cacheBucket.file(filePaths[i]).download({ destination: localPath });
    localFiles.push(localPath);
  }
  return localFiles;
}

async function processVideos(files, title, ranks, videoOrder) {
  const processedVideos = [];
  const sortedFiles = videoOrder.map(idx => files[idx]).filter(Boolean);
  const overlayPaths = [];

  try {
    // Process each video with progressively more ranks showing
    for (let i = 0; i < sortedFiles.length; i++) {
      const outputPath = `/tmp/processed_${i}_${uuidv4()}.mp4`;
      const ranksToShow = i + 1; // First video shows 1 rank, second shows 2, etc.
      
      console.log(`[processVideos] Creating overlay for video ${i} (showing ${ranksToShow} ranks)`);
      
      // Create overlay image for this video
      const overlayCanvas = createTextOverlayImage(title, ranks, ranksToShow);
      const overlayPath = `/tmp/overlay_${i}_${uuidv4()}.png`;
      
      // Save overlay as PNG
      const buffer = overlayCanvas.toBuffer('image/png');
      fs.writeFileSync(overlayPath, buffer);
      overlayPaths.push(overlayPath);
      
      console.log(`[processVideos] Overlaying image on video ${sortedFiles[i]}`);
      await addImageOverlay(sortedFiles[i], outputPath, overlayPath);
      
      processedVideos.push(outputPath);
    }

    return processedVideos;
  } finally {
    // Clean up overlay images
    overlayPaths.forEach(overlayPath => {
      try {
        if (fs.existsSync(overlayPath)) fs.unlinkSync(overlayPath);
      } catch (err) {
        console.error(`[processVideos] Failed to delete overlay ${overlayPath}:`, err);
      }
    });
  }
}

function addImageOverlay(inputPath, outputPath, overlayImagePath) {
  return new Promise((resolve, reject) => {
    console.log('[addImageOverlay] Start');

    const ffmpeg = spawn('ffmpeg', [
      '-i', inputPath,
      '-i', overlayImagePath,
      '-filter_complex', '[0:v]scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920[bg];[bg][1:v]overlay=0:0',
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
    const tempFiles = fs.readdirSync('/tmp').filter(f => f.includes('input_') || f.includes('processed_') || f.includes('final_') || f.includes('overlay_'));
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
