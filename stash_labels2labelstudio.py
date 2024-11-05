# %%
# LABEL_STUDIO_LOCAL_FILES_SERVING_ENABLED=true LABEL_STUDIO_LOCAL_FILES_DOCUMENT_ROOT=/mnt/nas/server/data/ LOCAL_FILES_SERVING_ENABLED=true label-studio


# %%
import json
import glob
from tqdm.auto import tqdm
from multiprocessing.pool import ThreadPool, Pool
import os
import urllib.parse
import uuid
from datetime import datetime, timezone

# %%
import json
import os

PATH_ROOT = "/mnt/nas/server/data/"
annotation_paths = sum([
    glob.glob("/mnt/nas/server/data/torrents/tv-whisparr/*/*.AI.json", recursive=True) +
    glob.glob("/mnt/nas/server/data/torrents/whisparr/*/*.AI.json", recursive=True) +
    
    glob.glob("/mnt/nas/server/data/media/tv-whisparr/*/*.AI.json", recursive=True) +
    glob.glob("/mnt/nas/server/data/media/whisparr/*/*.AI.json", recursive=True),
    
    # ==

    glob.glob("/mnt/nas/server/data/torrents/tv-whisparr/*.AI.json", recursive=True) +
    glob.glob("/mnt/nas/server/data/torrents/whisparr/*.AI.json", recursive=True) +
    
    glob.glob("/mnt/nas/server/data/media/tv-whisparr/*.AI.json", recursive=True) +
    glob.glob("/mnt/nas/server/data/media/whisparr/*.AI.json", recursive=True),
], [])


# annotation_path = "/mnt/nas/server/data/torrents/tv-whisparr/OfficePOV.2023.Mia.Evans.Masturbating.At.Work.XXX.1080p.MP4-P2P[XC]/officepov.2023.mia.evans.masturbating.at.work.xxx.mp4.AI.json"
# annotation_path = annotation_paths[0]


path2annotation = {
    annotation_path: json.load(open(annotation_path, 'r', encoding='utf-8'))
    for annotation_path in tqdm(annotation_paths, desc="loading annotations")
}
len_before_dropping = len(path2annotation)
path2annotation = {
    k: v for k, v in path2annotation.items()
    if os.path.exists(k.replace(".AI.json", ""))
}

print(f"dropped {len_before_dropping - len(path2annotation)} files")
len(path2annotation)

# %%
def convert_input_to_labelstudio_task(input_data: dict, filepath: str, task_id=None):
    # Initialize the output data as a list
    # Get video metadata
    video_metadata = input_data.get("video_metadata", {})
    duration = video_metadata.get("duration", None)

    # Create the task item
    task = {}

    # Assign an ID (e.g., 1)
    task["id"] = task_id

    # Prepare annotations
    annotations = []

    # Prepare the annotation
    annotation = {}

    # Assign an ID to the annotation
    annotation["id"] = 1

    # Set "completed_by", perhaps default to 1
    annotation["completed_by"] = 1

    # Prepare the "result" list
    result = []

    # Now, for each tag in input_data["tags"]
    tags = input_data.get("tags", {})
    for tag_name, tag_info in tags.items():
        time_frames = tag_info.get("time_frames", [])
        ai_model_name = tag_info.get("ai_model_name", "")
        # Get frame_interval from ai_model_config
        frame_interval = video_metadata.get("models", {}).get(ai_model_name, {}).get("ai_model_config", {}).get("frame_interval", 2.0)

        for time_frame in time_frames:
            # For each time frame, create a result item
            result_item = {}

            result_item["original_length"] = duration

            value = {}

            # Get start and end times
            start_time = time_frame.get("start")
            end_time = time_frame.get("end", None)
            if end_time is None:
                # If end_time is not provided, use frame_interval
                end_time = start_time + frame_interval

            value["start"] = start_time
            value["end"] = end_time
            value["channel"] = 0
            value["labels"] = [tag_name]

            result_item["value"] = value

            # Generate a unique ID for this result item
            result_item["id"] = str(uuid.uuid4())[:5]  # Shorten for readability

            # Set "from_name", "to_name", "type", "origin"
            result_item["from_name"] = "label"  # Adjust based on your labeling config
            result_item["to_name"] = "audio"    # Adjust based on your labeling config
            result_item["type"] = "labels"
            result_item["origin"] = "manual"

            # Append to result list
            result.append(result_item)

    # Complete the annotation
    annotation["result"] = result

    # Other fields in annotation
    annotation["was_cancelled"] = False
    annotation["ground_truth"] = False

    # Set timestamps
    # now = datetime.utcnow().isoformat() + 'Z'
    now = datetime.now(timezone.utc).isoformat()  # Updated to use timezone-aware datetime


    annotation["created_at"] = now
    annotation["updated_at"] = now

    # For lead_time, we can set to 0 or compute if needed
    annotation["lead_time"] = 0

    # Additional fields
    annotation["prediction"] = {}
    annotation["result_count"] = len(result)
    annotation["unique_id"] = str(uuid.uuid4())
    annotation["import_id"] = None
    annotation["last_action"] = None
    annotation["task"] = task["id"]
    annotation["project"] = 1
    annotation["updated_by"] = 1
    annotation["parent_prediction"] = None
    annotation["parent_annotation"] = None
    annotation["last_created_by"] = None

    # Add annotation to annotations list
    annotations.append(annotation)

    # Assign annotations to task
    task["annotations"] = annotations

    # Other fields in task
    # "file_upload", "data", etc.

    # For "file_upload", perhaps we can get the video filename from video_metadata
    # filename = video_metadata.get("filename", "video.mp4")
    task["file_upload"] = os.path.basename(filepath)

    # For "data", perhaps set "video_url"
    video_url = "/data/local-files/?d=" + urllib.parse.quote(filepath) 
    task["data"] = {"video_url": video_url}

    # Additional fields in task
    task["drafts"] = []
    task["predictions"] = []
    task["meta"] = {}
    task["created_at"] = now
    task["updated_at"] = now
    task["inner_id"] = task["id"]
    task["total_annotations"] = len(annotations)
    task["cancelled_annotations"] = 0
    task["total_predictions"] = 0
    task["comment_count"] = 0
    task["unresolved_comment_count"] = 0
    task["last_comment_updated_at"] = None
    task["project"] = 1
    task["updated_by"] = 1
    task["comment_authors"] = []
    return task



# %%
path, annotation = next(iter(path2annotation.items()))
path


def go(i__path_annotation):
    i, (path, annotation) = i__path_annotation
    # if os.path.exists(path.replace(".AI.json", ".label-studio.json")):
    #     return
    video_path = path.replace(".AI.json", "")
    output_data = convert_input_to_labelstudio_task(
        annotation,
        video_path.replace(PATH_ROOT, ""),
        task_id=i,
    )
    with open(video_path + ".label-studio.json", 'w') as f:
        json.dump([output_data], f, indent=4)
    return output_data

annotations = list(tqdm(
    Pool(16).imap(go, enumerate(path2annotation.items())),
    "converting to label-studio format",
    total=len(path2annotation),
))
len(annotations)

# %%
len(path2annotation)

# %%
path2labelstudio = {path.replace(".AI.json", ""): annotation for path, annotation in zip(path2annotation.keys(), annotations)}
next(iter(path2labelstudio.items()))

# %%

labelstudio_tasks = list(path2labelstudio.values())

print("saving labelstudio_tasks.json")
with open('labelstudio_tasks.json', 'w') as f:
    json.dump(labelstudio_tasks, f)


print("Sharding")
def save_shard(i):
    start = i * shard_size
    end = min(start + shard_size, len(labelstudio_tasks))
    with open(f'labelstudio_tasks-{start}-{end}.json', 'w') as f:
        json.dump(labelstudio_tasks[start:end], f)

shard_size = 300
with Pool() as pool:
    list(tqdm(
        pool.imap(save_shard, range((len(labelstudio_tasks) + shard_size - 1) // shard_size)),
        desc="sharding",
        total=(len(labelstudio_tasks) + shard_size - 1) // shard_size)
    )


# with open('labelstudio_tasks_895.json', 'w') as f:
#     json.dump(list(path2labelstudio.values())[894:1006], f)


# # %%
# query = "hentaied.24.06.14.eve.sweet.eves.ninth.gate.4k.mp4"
# # get index of JSON that contains query

# for i, (path, annotation) in enumerate(path2labelstudio.items()):
#     if query in json.dumps(annotation):
#         print(i, path, annotation['id'])
#         break

# # %%
# path2labelstudio.keys()
