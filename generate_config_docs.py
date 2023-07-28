#!/usr/bin/env python
import os
from pathlib import Path

from sparglim.config.configer import SparkEnvConfiger

_HERE = Path(os.path.abspath(__file__)).parent
TEMPLATE_PATH = _HERE / "config-template.md"
OUTPUT_PATH = _HERE / "config.md"


def _generate_env_config_docs(config: dict) -> str:
    docs = ""
    for spark_config, (env, default) in config.items():
        annotations = ""
        if isinstance(env, list):
            env = "` or `".join(env)
        if env.endswith("_LIST"):
            spark_config = spark_config.replace("list", "*")
            annotations = " A string seperated by `,` will be converted"
        docs += f"- `{env}`: `{spark_config}`, default: `{default}`.{annotations}\n"
    return docs


def generate_docs(target_configer_cls) -> str:
    docs = ""

    source_code_path = target_configer_cls.__module__.replace(".", "/") + ".py"
    docs += f"Source code: {source_code_path}\n\n"

    docs += f"Avaliable environment variables for {target_configer_cls.__name__}:\n\n"

    docs += f"Default config:\n\n"
    docs += _generate_env_config_docs(target_configer_cls.default_config_mapper)

    items = target_configer_cls.__dict__.items()
    config_map = {k: v for k, v in items if k.startswith("_") and isinstance(v, dict)}
    for config_suffix, config in config_map.items():
        docs += f"\n`config{config_suffix}()` can config following:\n\n"
        docs += _generate_env_config_docs(config)

    return docs


def generate_from_template(docs: str):
    print(f"Generating docs... {TEMPLATE_PATH.as_posix()} -> {OUTPUT_PATH.as_posix()}")
    print(docs)
    template = TEMPLATE_PATH.read_text()
    template = template.format(docs=docs)
    OUTPUT_PATH.write_text(template)


if __name__ == "__main__":
    docs = generate_docs(SparkEnvConfiger)
    generate_from_template(docs)
