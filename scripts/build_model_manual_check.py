"""
This script is used to build the model manually and check the results.
One config files names is uncommented to check model build
for that model.
"""

from bedrock.utils.config.usa_config import set_global_usa_config

config = "2025_usa_cornerstone_full_model_electricity_disaggregation"
# config = "2025_usa_cornerstone_v0_3"


def main() -> None:
    set_global_usa_config(config)
    from bedrock.transform.eeio.derived import (
        derive_Aq_usa,
        derive_B_usa_non_finetuned,
    )

    derive_Aq_usa()
    derive_B_usa_non_finetuned()
    print('Generated Aqset and B for ', config)


if __name__ == '__main__':
    main()
