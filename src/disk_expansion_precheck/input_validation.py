from __future__ import annotations

import re


REQUIRED_FIELDS = {
    "cloud_provider",
    "region",
    "instance_id",
    "disk_id",
    "target_size_gb",
    "os_type",
    "disk_role",
}

OPTIONAL_FIELDS = {
    "dist_name",
    "current_size_gb",
    "filesystem_type",
    "partition_scheme",
    "lvm_in_use",
    "service_criticality",
    "change_window",
    "provider_constraints",
    "asset_information",
    "topology_information",
}

SUPPLEMENTARY_FIELDS = {
    "zone_id",
    "instance_name",
    "expected_backup_policy",
    "quota_scope_hint",
    "temporary_disk_hints",
}

ALLOWED_FIELDS = REQUIRED_FIELDS | OPTIONAL_FIELDS | SUPPLEMENTARY_FIELDS

ENUM_FIELDS = {
    "cloud_provider": {"aliyun"},
    "os_type": {"linux", "windows"},
    "disk_role": {"system", "data"},
    "partition_scheme": {"MBR", "GPT"},
    "service_criticality": {"low", "medium", "high"},
}

REGION_PATTERN = re.compile(r"^[a-z]{2}(?:-[a-z0-9]+)+$")
INSTANCE_ID_PATTERN = re.compile(r"^i-[a-zA-Z0-9]+$")
DISK_ID_PATTERN = re.compile(r"^d-[a-zA-Z0-9]+$")


def validate_inputs(input_data: dict) -> dict:
    normalized_input = _normalize_input(input_data)
    unknown_fields = sorted(set(normalized_input.keys()) - ALLOWED_FIELDS)
    missing_fields = sorted(field for field in REQUIRED_FIELDS if _is_missing(normalized_input.get(field)))
    errors = []

    if unknown_fields:
        errors.append(f"存在未定义的输入字段：{', '.join(unknown_fields)}。")

    if missing_fields:
        errors.append(f"缺少必要输入字段：{', '.join(missing_fields)}。")

    errors.extend(_validate_enum_fields(normalized_input))
    errors.extend(_validate_numeric_fields(normalized_input))
    errors.extend(_validate_string_fields(normalized_input))
    errors.extend(_validate_format_fields(normalized_input))
    errors.extend(_validate_cross_field_rules(normalized_input))

    if errors:
        return {
            "valid": False,
            "status": "failed",
            "error": {
                "code": "invalid_input",
                "message": "；".join(errors),
            },
            "validation_summary": {
                "required_fields": sorted(REQUIRED_FIELDS),
                "optional_fields": sorted(OPTIONAL_FIELDS),
                "supplementary_fields": sorted(SUPPLEMENTARY_FIELDS),
            },
        }

    return {
        "valid": True,
        "normalized_input": normalized_input,
        "validation_summary": {
            "required_fields": sorted(REQUIRED_FIELDS),
            "provided_required_fields": sorted(field for field in REQUIRED_FIELDS if field in normalized_input),
            "provided_optional_fields": sorted(field for field in OPTIONAL_FIELDS if field in normalized_input),
            "provided_supplementary_fields": sorted(
                field for field in SUPPLEMENTARY_FIELDS if field in normalized_input
            ),
        },
    }


def _normalize_input(input_data: dict) -> dict:
    normalized_input = {}

    for key, value in input_data.items():
        if value is None:
            continue
        if isinstance(value, str):
            value = value.strip()
        normalized_input[key] = value

    return normalized_input


def _validate_enum_fields(input_data: dict) -> list[str]:
    errors: list[str] = []

    for field, allowed_values in ENUM_FIELDS.items():
        value = input_data.get(field)
        if value is None:
            continue
        if value not in allowed_values:
            errors.append(
                f"字段 {field} 的值 {value!r} 不合法，可用值为：{', '.join(sorted(allowed_values))}。"
            )

    lvm_in_use = input_data.get("lvm_in_use")
    if lvm_in_use is not None and not isinstance(lvm_in_use, bool):
        errors.append("字段 lvm_in_use 的值类型不合法，应为布尔值。")

    return errors


def _validate_numeric_fields(input_data: dict) -> list[str]:
    errors: list[str] = []

    target_size_gb = input_data.get("target_size_gb")
    if target_size_gb is not None:
        if not isinstance(target_size_gb, int):
            errors.append("字段 target_size_gb 的值类型不合法，应为整数。")
        elif target_size_gb <= 0:
            errors.append("字段 target_size_gb 的值必须大于0。")

    current_size_gb = input_data.get("current_size_gb")
    if current_size_gb is not None:
        if not isinstance(current_size_gb, int):
            errors.append("字段 current_size_gb 的值类型不合法，应为整数。")
        elif current_size_gb <= 0:
            errors.append("字段 current_size_gb 的值必须大于0。")

    return errors


def _validate_string_fields(input_data: dict) -> list[str]:
    errors: list[str] = []

    for field, value in input_data.items():
        if field in {"target_size_gb", "current_size_gb", "lvm_in_use"}:
            continue
        if not isinstance(value, str):
            errors.append(f"字段 {field} 的值类型不合法，应为字符串。")
            continue
        if not value.strip():
            errors.append(f"字段 {field} 的值不能为空。")

    return errors


def _validate_format_fields(input_data: dict) -> list[str]:
    errors: list[str] = []

    region = input_data.get("region")
    if isinstance(region, str) and region and not REGION_PATTERN.match(region):
        errors.append("字段 region 的格式不合法，应类似 cn-hangzhou。")

    instance_id = input_data.get("instance_id")
    if isinstance(instance_id, str) and instance_id and not INSTANCE_ID_PATTERN.match(instance_id):
        errors.append("字段 instance_id 的格式不合法，应类似 i-bp1f2g3h4i5j6k7l。")

    disk_id = input_data.get("disk_id")
    if isinstance(disk_id, str) and disk_id and not DISK_ID_PATTERN.match(disk_id):
        errors.append("字段 disk_id 的格式不合法，应类似 d-bp18xq9zk7a1b2c3。")

    return errors


def _validate_cross_field_rules(input_data: dict) -> list[str]:
    errors: list[str] = []

    current_size_gb = input_data.get("current_size_gb")
    target_size_gb = input_data.get("target_size_gb")
    if isinstance(current_size_gb, int) and isinstance(target_size_gb, int):
        if target_size_gb <= current_size_gb:
            errors.append("字段 target_size_gb 必须大于 current_size_gb，才能构成有效扩容目标。")

    os_type = input_data.get("os_type")
    filesystem_type = input_data.get("filesystem_type")
    if isinstance(os_type, str) and isinstance(filesystem_type, str):
        normalized_fs = filesystem_type.lower()
        if os_type == "windows" and normalized_fs not in {"ntfs", "refs"}:
            errors.append("字段 filesystem_type 与 os_type=windows 不匹配，建议使用 ntfs 或 refs。")
        if os_type == "linux" and normalized_fs in {"ntfs", "refs", "fat32"}:
            errors.append("字段 filesystem_type 与 os_type=linux 不匹配，请提供常见 Linux 文件系统类型。")

    dist_name = input_data.get("dist_name")
    if isinstance(os_type, str) and isinstance(dist_name, str):
        normalized_dist = dist_name.lower()
        if os_type == "windows" and "windows" not in normalized_dist:
            errors.append("字段 dist_name 与 os_type=windows 不匹配。")
        if os_type == "linux" and "windows" in normalized_dist:
            errors.append("字段 dist_name 与 os_type=linux 不匹配。")

    return errors


def _is_missing(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    return False
