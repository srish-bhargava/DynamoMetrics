#!/usr/bin/env python3

import os
import asyncio
import matplotlib.pyplot as plt
from tqdm import tqdm
import math


def format_params(params):
    args = ", ".join(f"{key}: {value}" for key, value in params.items())
    param_str = "%Params{" + args + "}"
    return f"MeasureStatistics.measure_output_csv({param_str})"


async def make_measurement(params, progress=None):
    async with measurement_semaphore:
        cmd = f"mix run -e '{format_params(params)}'"
        proc = await asyncio.create_subprocess_shell(
            cmd, stdout=asyncio.subprocess.PIPE
        )

        stdout, _stderr = await proc.communicate()
        output = stdout.decode()

        parsed_output = [float(elem) for elem in output.split(", ")]

        if progress is not None:
            progress.update()

        return parsed_output


async def make_measurement_average(params, reps, progress=None):
    if reps == 1:
        return await make_measurement(params, progress=progress)
    results = await asyncio.gather(
        *(make_measurement(params, progress=progress) for _ in range(reps))
    )
    averages = [
        round(math.sqrt(sum(val * val for val in field_vals) / reps), 4)
        for field_vals in zip(*results)
    ]
    return averages


async def plot(param_key, param_values, reps=1, overrides={}):
    standard_params = {
        "duration": "10_000",
        # fuzzer params
        "request_rate": "{10, 20}",
        "drop_rate": "0.01",
        "mean_delay": "50",
        "tt_fail": "10_000_000",
        # test params
        "cluster_size": "20",
        "num_keys": "1000",
        "num_clients": "20",
        # dynamo params
        "n": "3",
        "r": "2",
        "r": "2",
        "request_timeout": "150",
        "coordinator_timeout": "300",
        "total_redirect_timeout": "300",
        "alive_check_interval": "50",
        "replica_sync_interval": "200",
        **overrides,
    }

    def to_str(val):
        if type(val) in [int, float]:
            return str(val)
        elif type(val) == tuple:
            return "{" + ",".join(to_str(v) for v in val) + "}"
        else:
            raise ValueError(f"Unhandled type {type(val)} of {val}")

    with tqdm(total=len(param_values) * reps) as progress:
        results = await asyncio.gather(
            *(
                make_measurement_average(
                    {**standard_params, param_key: to_str(param_value)},
                    reps=reps,
                    progress=progress,
                )
                for param_value in param_values
            )
        )

    plt.figure(1)
    plt.suptitle(f"Against {param_key}")

    for idx, item in enumerate(
        zip(["availability", "inconsistency", "stale reads"], *results)
    ):
        (yname, *ys) = item
        plt.subplot(1, 3, idx + 1)
        plt.xlabel(param_key)
        plt.ylabel(yname)
        plt.plot(param_values, list(ys))
        if yname == "availability":
            plt.ylim(bottom=0, top=105)
        elif yname == "inconsistency":
            plt.ylim(bottom=0, top=50)
        elif yname == "stale reads":
            plt.ylim(bottom=0, top=1)

    plt.show()


async def main():
    global measurement_semaphore
    measurement_semaphore = asyncio.Semaphore(50)
    # await plot("duration", [i * 1000 for i in range(10, 21)], reps=5)
    # await plot("drop_rate", [i * 0.01 for i in range(1, 21)], reps=10)
    # await plot("tt_fail", [10 ** 4 / i for i in range(1, 100)], reps=10)
    await plot(
        "request_rate",
        [(i, i) for i in [10, 20, 40, 80, 160, 320, 640, 1280, 2560, 5120, 10240]],
        reps=50,
        overrides={"duration": "5000"},
    )


if __name__ == "__main__":
    os.chdir(os.path.join(os.path.dirname(__file__), os.pardir))
    asyncio.run(main())
