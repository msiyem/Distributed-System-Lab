import multiprocessing as mp

import os

DOCUMENTS = [
    "distributed systems are fun",
    "map reduce is a programming model",
    "the master splits input into map tasks",
    "reduce tasks sum the counts from map tasks",
    "fun fun fun distributed computing",
]

NUM_REDUCERS = 3   

def map_task(doc_id, text):
    print(f"[MAP worker pid={os.getpid()}] mapping doc {doc_id}: {text!r}")
    pairs = []
    for word in text.split():
        pairs.append((word, 1))
    return pairs

def shuffle(all_mapped_pairs, num_reducers):
    groups = {}
    for pairs in all_mapped_pairs:
        for word, count in pairs:
            groups.setdefault(word, []).append(count)

    buckets = [{} for _ in range(num_reducers)]
    for word, counts in groups.items():
        bucket_id = hash(word) % num_reducers
        buckets[bucket_id][word] = counts
    return buckets

def reduce_task(bucket_id, bucket):
    print(f"[REDUCE worker pid={os.getpid()}] reducing bucket {bucket_id} "
          f"({len(bucket)} distinct words)")
    result = {}
    for word, counts in bucket.items():
        result[word] = sum(counts)
    return result
 
def main():
    print("[MASTER] Splitting input into", len(DOCUMENTS), "map tasks")


    with mp.Pool(processes=4) as pool:
        map_results = pool.starmap(
            map_task, [(i, doc) for i, doc in enumerate(DOCUMENTS)]
        )
    print("[MASTER] Map phase done. Starting shuffle...")

    buckets = shuffle(map_results, NUM_REDUCERS)
    print(f"[MASTER] Shuffle done. Created {NUM_REDUCERS} reduce partitions.")

    with mp.Pool(processes=NUM_REDUCERS) as pool:
        reduce_results = pool.starmap(
            reduce_task, list(enumerate(buckets))
        )

    final_counts = {}
    for partial in reduce_results:
        final_counts.update(partial)

    print("\n[MASTER] FINAL WORD COUNTS:")
    for word, count in sorted(final_counts.items(), key=lambda kv: -kv[1]):
        print(f"  {word:12s} {count}")
        
if __name__ == "__main__":
    main()
    