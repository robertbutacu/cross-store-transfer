name := "cross-store-transfer"

version := "0.1"

scalaVersion := "2.13.1"

libraryDependencies ++= Seq(
  "com.github.fs2-blobstore" %% "core" % "0.6.+",
  "com.github.fs2-blobstore" %% "sftp" % "0.6.+",
  "com.github.fs2-blobstore" %% "s3" % "0.6.+"
)