import multiprocessing as mp
import os

docs = [
    "distributed systems are fun",
    "map reduce is a programming model",
    "the master splits input into map tasks",
    "reduce tasks sum the counts from map tasks",
    "fun fun fun distributed computing",
]

num_reducers = 3


def map_task(doc_id, text):
    print(f"[map worker pid={os.getpid()}] mapping doc {doc_id}: {text!r}")

    pairs = []

    for word in text.split():
        length = len(word)
        pairs.append((length, 1))

    return pairs


def shuffle(all_map_pairs, num_reducers):
    groups = {}

    for pairs in all_map_pairs:
        for length, count in pairs:
            groups.setdefault(length, []).append(count)

    buckets = [{} for _ in range(num_reducers)]

    for length, count in groups.items():
        bucket_id = length % num_reducers
        buckets[bucket_id][length] = count

    return buckets


def reduce_task(bucket_id, bucket):
    print(
        f"[reduce worker pid={os.getpid()}] "
        f"reducing bucket {bucket_id} {len(bucket)} distinct lengths"
    )

    result = {}

    for length, count in bucket.items():
        result[length] = sum(count)

    return result


def main():

    print(f"[master] splitting the input into {len(docs)} map tasks")

    # MAP
    with mp.Pool(processes=3) as pool:
        map_results = pool.starmap(
            map_task,
            [(i, doc) for i, doc in enumerate(docs)]
        )

    print("[master] map phase done. starting shuffle ...")

    # SHUFFLE
    buckets = shuffle(map_results, num_reducers)

    print(
        f"[master] shuffle done. "
        f"created {num_reducers} reduce partitions"
    )

    # REDUCE
    with mp.Pool(processes=num_reducers) as pool:
        reduce_results = pool.starmap(
            reduce_task,
            list(enumerate(buckets))
        )

    # FINAL RESULT
    final_counts = {}

    for partial in reduce_results:
        final_counts.update(partial)

    print("\n[master] final word length counts:")

    for length, count in sorted(final_counts.items()):
        print(f"length {length:2d} : {count}")


if __name__ == "__main__":
    main()
