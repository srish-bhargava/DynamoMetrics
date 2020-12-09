#!/usr/bin/env python3

import os
import asyncio


def format_params(params):
    args = ", ".join(f"{key}: {value}" for key, value in params.items())
    param_str = "%Params{" + args + "}"
    return f"MeasureStatistics.measure_output_csv({param_str})"


async def make_measurement(params):
    cmd = f"mix run -e '{format_params(params)}'"
    proc = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE)

    stdout, _stderr = await proc.communicate()
    output = stdout.decode()

    parsed_output = [float(elem) for elem in output.split(", ")]
    return parsed_output


async def make_measurement_average(params, reps):
    if reps == 1:
        return await make_measurement(params)
    results = await asyncio.gather(*(make_measurement(params) for _ in range(reps)))
    averages = [round(sum(at_index) / len(at_index), 4) for at_index in zip(*results)]
    return averages


async def analyse():
    reps = 10

    standard_params = {
        "duration": "10_000",
        # fuzzer params
        "request_rate": "{10, 20}",
        "drop_rate": "0.05",
        "mean_delay": "100",
        "tt_fail": "20_000",
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
    }

    more_clients_params = {**standard_params, "num_clients": "50"}

    experiments = {"standard": standard_params, "more clients": more_clients_params}

    async def run_experiment(experiment, reps=1):
        name, params = experiment
        result = await make_measurement_average(params, reps=reps)
        return (name, result)

    results = await asyncio.gather(
        *(run_experiment(experiment, reps=reps) for experiment in experiments.items())
    )
    return {name: result for (name, result) in results}


if __name__ == "__main__":
    os.chdir(os.path.join(os.path.dirname(__file__), os.pardir))
    result = asyncio.run(analyse())
    print(result)
