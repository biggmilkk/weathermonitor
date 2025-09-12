const path = require("path");

module.exports = {
  entry: "./src/index.tsx",
  output: {
    filename: "index.js",
    path: path.resolve(__dirname, "build"),
    libraryTarget: "umd",
  },
  resolve: { extensions: [".ts", ".tsx", ".js"] },
  module: {
    rules: [{ test: /\.tsx?$/, use: "ts-loader", exclude: /node_modules/ }],
  },
  devServer: {
    static: path.join(__dirname, "build"),
    port: 3001,
    hot: true,
    allowedHosts: "all",
  },
};
