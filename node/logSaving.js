const express = require("express");
const cors = require("cors");
const fs = require("fs");
const path = require("path");

const app = express();
const port = 3000;

app.use(
  cors({
    origin: "http://127.0.0.1:5500",
    methods: ["GET", "POST", "OPTIONS"],
  })
);

app.use(express.json());

app.post("/log", (req, res) => {
  const { productName, productId, productPrice, actionName, currentPath } =
    req.body;
  const date = new Date();
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  const hour = String((date.getHours() + 2) % 24).padStart(2, "0");
  const minute = String(date.getMinutes()).padStart(2, "0");
  const second = String(date.getSeconds()).padStart(2, "0");

  // const dateStr = `${year}${month}${day}${hour}`;
  // const logDir = path.join(__dirname, "logs", dateStr);
  const logDir = path.join(__dirname, "logs");
  const logFilename = `${year}${month}${day}${hour}${minute}.txt`;
  const logEntry = `${year}-${month}-${day} ${hour}:${minute}:${second} ${productName}|${productId}|${productPrice}|${actionName}|${currentPath}\n`;

  // Create the directory if it does not exist
  // fs.mkdir(logDir, { recursive: true }, (err) => {
  //   if (err) {
  //     console.error("Error creating directory:", err);
  //     res
  //       .status(500)
  //       .json({ success: false, message: "Error creating directory" });
  //     return;
  //   }

  // Write the log entry to a file
  fs.appendFile(path.join(logDir, logFilename), logEntry, (err) => {
    if (err) {
      console.error("Error writing log:", err);
      res.status(500).json({ success: false, message: "Error writing log" });
    } else {
      console.log("Log saved:", logEntry);
      res
        .status(200)
        .json({ success: true, message: "Log saved successfully" });
    }
  });
});

app.listen(port, () => {
  console.log(`Server running at http://localhost:${port}`);
});
