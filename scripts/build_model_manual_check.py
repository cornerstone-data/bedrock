"""
This script is used to build the model manually and check the results.
One config files names is uncommented to check model build
for that model.
"""

from bedrock.utils.config.usa_config import set_global_usa_config

# config = "useeio_phoebe_23"
# config = "useeio_phoebe_23_restore_schema_and_ghg"
# config = "useeio_phoebe_23_restore_cornerstone_B"
# config = "useeio_phoebe_23_restore_iot_redefinition"
# config = "useeio_phoebe_23_restore_ghg"
# config = 'useeio_phoebe_23_restore_scrap'
# config = "2025_usa_cornerstone_v0_2"
# config = "2025_usa_cornerstone_taxonomy"
# config = "2025_usa_cornerstone_taxonomy_and_waste_disagg"
# config = "2025_usa_cornerstone_B_transformation_and_waste_disaggregation"
# config = "2025_usa_cornerstone_ghg"
# config = "2025_usa_cornerstone_fbs_schema"
# config = "2025_usa_cornerstone_fbs_schema_and_waste_disagg"
# config = "2025_usa_cornerstone_fbs_schema_and_B_transformation_and_waste_disaggregation"
# config = "2025_usa_cornerstone_fbs_schema_and_ghg"
config = "2025_usa_cornerstone_full_model_electricity_disaggregation"


def main() -> None:
    set_global_usa_config(config)
    from bedrock.transform.eeio.derived import (
        derive_Aq_usa,
        derive_B_usa_non_finetuned,
    )

    derive_Aq_usa()
    derive_B_usa_non_finetuned()
    print('Generated Aq set and B for config: ', config)


if __name__ == '__main__':
    main()
