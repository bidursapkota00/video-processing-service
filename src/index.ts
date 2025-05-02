import express from "express";
import {
  setupDirectories,
  downloadRawVideo,
  convertVideo,
  deleteRawVideo,
  deleteProcessedVideo,
  uploadProcessedVideo,
} from "./storage";
import { setVideo, getVideo } from "./firestore";

setupDirectories();

const app = express();
app.use(express.json());

const port = process.env.PORT || 3000;

app.post("/process-video", async (req, res) => {
  // Get the bucket and filename from the Cloud Pub/Sub message
  let data;
  try {
    const message = Buffer.from(req.body.message.data, "base64").toString(
      "utf8"
    );
    data = JSON.parse(message);
    if (!data.name) {
      throw new Error("Invalid message payload received.");
    }
  } catch (error) {
    console.error(error);
    res.status(400).send("Bad Request: missing filename.");
    return;
  }
  const inputFileName = data.name;
  const outputFileName = `processed-${inputFileName}`;
  const videoId = inputFileName.split(".")[0];

  const video = await getVideo(videoId);

  if (video?.status === "processed") {
    res.status(200).send("Video already processed");
    return;
  } else if (video?.status === "processing") {
    res.status(400).send("Bad Request: video already processing");
    return;
  } else {
    await setVideo(videoId, {
      id: videoId,
      uid: videoId.split("-")[0],
      status: "processing",
    });
  }

  // Download the raw video from Cloud Storage
  await downloadRawVideo(inputFileName);

  // Convert the video to 360p
  try {
    await convertVideo(inputFileName, outputFileName);
  } catch (err) {
    await setVideo(videoId, {
      status: undefined,
    });
    await Promise.all([
      deleteRawVideo(inputFileName),
      deleteProcessedVideo(outputFileName),
    ]);
    console.error(err);
    res.status(500).send("Internal Server Error: video processing failed.");
    return;
  }

  // Upload the processed video to Cloud Storage
  await uploadProcessedVideo(outputFileName);

  await setVideo(videoId, {
    status: "processed",
    filename: outputFileName,
  });

  await Promise.all([
    deleteRawVideo(inputFileName),
    deleteProcessedVideo(outputFileName),
  ]);
  res.status(200).send("Processing finished successfully");
});

app.listen(port, () => {
  console.log(`Video processing service listening at http://localhost:${port}`);
});
