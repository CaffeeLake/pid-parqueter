from datetime import datetime
from glob import glob as globglogabgalab
from os import path
from random import choice
from tqdm import tqdm
from uuid import uuid1
import argparse
import pandas as pd

def create_pickapic_dataframe():
    return pd.DataFrame(
        columns=[
            "are_different",
            "best_image_uid",
            "caption",
            "created_at",
            "has_label",
            "image_0_uid",
            "image_0_url",
            "image_1_uid",
            "image_1_url",
            "jpg_0",
            "jpg_1",
            "label_0",
            "label_1",
            "model_0",
            "model_1",
            "ranking_id",
            "user_id",
            "num_example_per_prompt",
        ],
    )

def generate_queries(img_dirs):
    images = []
    exts = ["**/*.png", "**/*.jpg", "**/*.jpeg"]
    for files in exts:
        images.extend(globglogabgalab(files, root_dir=img_dirs, recursive=True))
    return images

def add_to_pickapic_dataframe(preference_df, image0_dir, image0_path, image1_dir, image1_path):

    image_path0 = path.join(image0_dir, image0_path)
    image_path1 = path.join(image1_dir, image1_path)
    are_different = not image_path0 == image_path1
    created_at = datetime.now()
    has_label = True
    image_0_uid = str(uuid1())
    image_0_url = image0_path
    image_1_uid = str(uuid1())
    image_1_url = image1_path
    best_image_uid = str(image_1_uid)
    with open(image_path0, "rb") as img0:
        jpg_0 = img0.read()

    with open(image_path1, "rb") as img1:
        jpg_1 = img1.read()

    with open(image_path1 + ".txt", "rt") as prompt1:
        prompt = prompt1.read()

    model0 = "dummy"
    model1 = "reference"
    ranking_id = 0
    user_id = 0
    num_example_per_prompt = 1

    label0, label1 = [0, 1]

    preference_df.loc[len(preference_df.index)] = [
        are_different,
        best_image_uid,
        prompt,
        created_at,
        has_label,
        image_0_uid,
        image_0_url,
        image_1_uid,
        image_1_url,
        jpg_0,
        jpg_1,
        label0,
        label1,
        model0,
        model1,
        ranking_id,
        user_id,
        num_example_per_prompt,
    ]

    return preference_df

def main(args):
    preference_df = create_pickapic_dataframe()
    img0_paths = generate_queries(args.b)
    img1_paths = generate_queries(args.i)
    for image1_path in tqdm(img1_paths):
        image0_path = choice(img0_paths)
        add_to_pickapic_dataframe(preference_df, args.b, image0_path, args.i, image1_path)
    preference_df.to_parquet(args.o)

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-b", type=str, default="dummy")
    parser.add_argument("-i", type=str, default="reference")
    parser.add_argument("-o", type=str, default="export.parquet")
    args = parser.parse_args()
    main(args)
