import yaml, datetime, json


# Custom JSON serializer for datetime.date
def custom_serializer(obj):
    if isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


# Config variables
def flatten_config(prefix, config):
    for key, value in config.items():
        if isinstance(value, dict):
            flatten_config(f"{prefix}{key.upper()}_", value)
        else:
            print(
                f"{prefix}{key.upper()}={json.dumps(value, default=custom_serializer)}"
            )


def get_config():
    with open("../config.yml") as f:
        config = yaml.safe_load(f)

    return config


def main():
    config = get_config()

    flatten_config("", config)

    # Runtime variables
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    print(f"TIMESTAMP={timestamp}")


if __name__ == "__main__":
    main()
