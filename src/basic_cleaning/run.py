#!/usr/bin/env python

import argparse
import logging
import pandas as pd
import wandb
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    logger.info("Downloading artifact")
    artifact = run.use_artifact(args.input_artifact)
    artifact_dir = artifact.download()
    artifact_path = os.path.join(artifact_dir, args.input_artifact.split(":")[0].split("/")[-1].replace(".csv", "") + ".csv")
    
    # في حال ما اشتغل المسار السابق جرب مباشرة
    if not os.path.exists(artifact_path):
        files = os.listdir(artifact_dir)
        artifact_path = os.path.join(artifact_dir, files[0])

    df = pd.read_csv(artifact_path)

    df = df[df["price"].between(args.min_price, args.max_price)].copy()

    df["last_review"] = pd.to_datetime(df["last_review"])

    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()

    df.to_csv("clean_sample.csv", index=False)

    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )

    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Basic data cleaning step")

    parser.add_argument("--input_artifact", type=str, required=True)
    parser.add_argument("--output_artifact", type=str, required=True)
    parser.add_argument("--output_type", type=str, required=True)
    parser.add_argument("--output_description", type=str, required=True)
    parser.add_argument("--min_price", type=float, required=True)
    parser.add_argument("--max_price", type=float, required=True)

    args = parser.parse_args()

    go(args)