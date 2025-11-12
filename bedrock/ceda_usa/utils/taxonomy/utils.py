from __future__ import annotations

import json
import logging
import os
import typing as ta

import numpy as np

logger = logging.getLogger(__name__)

T0 = ta.TypeVar("T0", bound=str)
T1 = ta.TypeVar("T1", bound=str)
T2 = ta.TypeVar("T2", bound=str)


class MappingEntry(ta.TypedDict, ta.Generic[T0]):
    label: str
    BEA_CODES: ta.Dict[T0, float]


def traverse(
    m0_1: ta.Dict[T0, ta.List[T1]],
    m1_2: ta.Dict[T1, ta.List[T2]],
) -> ta.Dict[T0, ta.List[T2]]:
    """
    Traverse a map of lists to another map of lists
    """
    m0_2: ta.Dict[T0, ta.List[T2]] = {}
    for t0, t1s in m0_1.items():
        m0_2[t0] = []

        if not isinstance(t1s, list):
            t1s = [t1s]

        for t1 in t1s:
            t2s = m1_2.get(t1, [])
            if not isinstance(t2s, list):
                t2s = [t2s]

            m0_2[t0].extend(t2s)

    # handle duplicates
    for t0, t2s in m0_2.items():
        m0_2[t0] = list(set(t2s))
    return m0_2


def reverse(
    m0_1: ta.Dict[T0, ta.List[T1]], new_domain: ta.Optional[ta.AbstractSet[T1]]
) -> ta.Dict[T1, ta.List[T0]]:
    """
    Reverse a map of lists
    """
    m1_0: ta.Dict[T1, ta.List[T0]] = {}
    for t0, t1s in m0_1.items():
        if not isinstance(t1s, list):
            t1s = [t1s]
        for t1 in t1s:
            if t1 in m1_0:
                m1_0[t1].append(t0)
            else:
                m1_0[t1] = [t0]

    # handle duplicates
    for t1, t0s in m1_0.items():
        m1_0[t1] = list(set(t0s))

    if new_domain is not None:
        assert all(
            t1 in new_domain for t1 in m1_0.keys()
        ), "Found invalid key in reverse mapping that is not in new_domain."
    return m1_0


def validate_mapping(
    m: ta.Mapping[T0, ta.Union[T1, ta.List[T1]]],
    domain: ta.AbstractSet[T0],
    codomain: ta.AbstractSet[T1],
    check_domain_equal: bool = True,
    dangerously_skip_empty_mapping_check: bool = False,
) -> None:
    if set(m.keys()) != domain:
        extra = set(m.keys()) - domain
        missing = domain - set(m.keys())
        if check_domain_equal:
            raise AssertionError(
                f"Set of keys != domain. Extra: {extra}, Missing: {missing}"
            )
        if extra:
            raise AssertionError(
                f"Set of keys != domain. Extra: {extra}, Missing: {missing}"
            )
    for lst in m.values():
        if (
            not dangerously_skip_empty_mapping_check
            and isinstance(lst, list)
            and len(lst) == 0
        ):
            raise AssertionError("Found empty list in mapping.")
        if not isinstance(lst, list):
            lst = [lst]
        for code in lst:
            if code not in codomain:
                raise AssertionError(
                    f"Found invalid value in mapping that is not in codomain: {code}"
                )


def validate_weighted_mapping_with_entries(
    m: ta.Mapping[T0, MappingEntry[T1]],
    *,
    domain: ta.AbstractSet[T0],
    codomain: ta.AbstractSet[T1],
    if_fail: ta.Literal["warning", "exception"] = "exception",
) -> None:
    validate_weighted_mapping(
        {k: v["BEA_CODES"] for k, v in m.items()},
        domain=domain,
        codomain=codomain,
        if_fail=if_fail,
    )


def validate_weighted_mapping(
    m: ta.Mapping[T0, ta.Dict[T1, float]],
    *,
    domain: ta.AbstractSet[T0],
    codomain: ta.AbstractSet[T1],
    if_fail: ta.Literal["warning", "exception"] = "exception",
) -> None:
    try:
        assert_sets_equal(expected=domain, actual=set(m.keys()), if_fail=if_fail)
    except AssertionError as e:
        raise AssertionError("Domain mismatch") from e

    range_set: set[T1] = set()
    for weights in m.values():
        range_set.update(weights.keys())
        assert len(weights) == 0 or np.isclose(
            sum(weights.values()), 1.0
        ), f"Weights do not sum to 1 got {sum(weights.values())}"

    try:
        assert_sets_equal(
            expected=codomain, actual=range_set, missing_ok=True, if_fail=if_fail
        )
    except AssertionError as e:
        raise AssertionError("Codomain mismatch") from e


def assert_sets_equal(
    *,
    expected: ta.AbstractSet[ta.Any],
    actual: ta.AbstractSet[ta.Any],
    missing_ok: bool = False,
    if_fail: ta.Literal["warning", "exception"] = "exception",
) -> None:
    if expected == actual:
        return

    def err_func(msg: str) -> None:
        if if_fail == "exception":
            raise AssertionError(msg)
        elif if_fail == "warning":
            logger.warning(msg)
        else:
            ta.assert_never(if_fail)
            raise RuntimeError("unexpected")

    missing = expected - actual
    extra = actual - expected
    if extra or (missing and not missing_ok):
        missing_msg = f"missing: {missing}" if missing and not missing_ok else ""
        err_func(f"Sets not equal. {missing_msg} extra: {extra}")


def generate_typescript_map(
    weighted_mapping: ta.Dict[ta.Any, ta.Dict[ta.Any, float]],
    code_labels: ta.Dict[ta.Any, str],
    ts_var_name: str,
    include_file_header: bool = True,
    filename: str = "",
) -> str:
    # export const NAICS_CODES: {
    #   [key: string]: {
    #     label: string;
    #     BEA_CODES: WeightedBeaCodes;
    #   };
    # } = {
    #   ["111110"]:{ label: "Soybean Farming", BEA_CODES: {'1111A0': 1.0} },
    #   ["111120"]:{ label: "Oilseed (except Soybean) Farming", BEA_CODES: {'1111A0': 1.0} },

    mapping_entries_list = []

    def dict_sort(d: ta.Dict[ta.Any, ta.Any]) -> ta.Dict[ta.Any, ta.Any]:
        return {k: d[k] for k in sorted(d)}

    for k, bea_code_dict in dict_sort(weighted_mapping).items():
        mapping_entries_list.append(
            f'["{k}"]:{{ label: "{code_labels[k]}", BEA_CODES: {json.dumps(dict_sort(bea_code_dict))} }}'
        )

    mapping_entries = ",\n".join(mapping_entries_list)

    ts_string = ""
    if include_file_header:
        ts_string += f"""
        /*
            This file is semi-automatically generated from
            cliq/taxonomy/scripts/{filename} in the cliq repo
            Do not manually modify this file
        */
        import type {{ WeightedBeaCodes }} from "./industryCodeUtils";
        """
    ts_string += f"""
    export const {ts_var_name}: {{
      [key: string]: {{
        label: string;
        BEA_CODES: WeightedBeaCodes;
      }};
    }} = {{
        {mapping_entries}
    }}
    """
    return ts_string


def generate_check_json(
    filename: str,
    out_dir: str,
    weighted_mapping: ta.Dict[ta.Any, ta.List[ta.Any]],
    src_labels: ta.Dict[ta.Any, str],
    dst_labels: ta.Dict[ta.Any, str],
) -> None:
    check_map = {}
    for k, v in weighted_mapping.items():
        check_map[k] = {
            "label": src_labels[k],
            "CEDA_V5": [{"code": ceda_k, "label": dst_labels[ceda_k]} for ceda_k in v],
        }

    output_file = os.path.join(out_dir, filename)

    with open(
        output_file,
        "w+",
    ) as f:
        json.dump(check_map, fp=f, indent=2)

        print(f"check json saved to: {output_file}")


def get_weightings(
    mapping: ta.Mapping[T0, ta.Iterable[T1]],
    weights: ta.Mapping[T1, float],
    max_mapped: ta.Optional[int] = 10,
    raise_empty_mapping_error: bool = True,
) -> ta.Dict[T0, ta.Dict[T1, float]]:
    weighted_mapping: ta.Dict[T0, ta.Dict[T1, float]] = {}

    for k, vs in mapping.items():
        if not vs and raise_empty_mapping_error:
            raise RuntimeError(f"Empty mapping for {k}")

        k_weights = {v: weights[v] for v in vs}

        if max_mapped and len(k_weights) > max_mapped:
            # this is to prevent a single nace code from producing very large
            # footprints when we then estimate for a large number of ceda codes
            k_weights = dict(
                sorted(k_weights.items(), key=lambda item: item[1], reverse=True)[
                    :max_mapped
                ]
            )

        k_weight_sum = sum(k_weights.values())
        k_weights_dict = {k: round(v / k_weight_sum, 8) for k, v in k_weights.items()}

        weighted_mapping[k] = k_weights_dict

    return weighted_mapping
