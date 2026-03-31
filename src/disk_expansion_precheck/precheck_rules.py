from __future__ import annotations


def evaluate_optional_input_consistency(
    input_data: dict,
    instance: dict,
    disk: dict,
) -> tuple[list[str], str]:
    findings: list[str] = []
    status = "pass"

    required_findings, required_status = _check_required_input_consistency(input_data, instance, disk)
    findings.extend(required_findings)
    status = _merge_status(status, required_status)

    size_findings, size_status = _check_current_size_consistency(input_data, disk)
    findings.extend(size_findings)
    status = _merge_status(status, size_status)

    dist_findings, dist_status = _check_dist_name_consistency(input_data, instance)
    findings.extend(dist_findings)
    status = _merge_status(status, dist_status)

    os_layout_findings, os_layout_status = _check_os_layout_hints(input_data)
    findings.extend(os_layout_findings)
    status = _merge_status(status, os_layout_status)

    return findings, status


def _check_required_input_consistency(
    input_data: dict,
    instance: dict,
    disk: dict,
) -> tuple[list[str], str]:
    findings: list[str] = []
    status = "pass"

    target_size_findings, target_size_status = _check_target_size_consistency(input_data, disk)
    findings.extend(target_size_findings)
    status = _merge_status(status, target_size_status)

    os_type_findings, os_type_status = _check_os_type_consistency(input_data, instance)
    findings.extend(os_type_findings)
    status = _merge_status(status, os_type_status)

    return findings, status


def _check_target_size_consistency(input_data: dict, disk: dict) -> tuple[list[str], str]:
    findings: list[str] = []
    actual_size_gb = disk.get("Size")
    target_size_gb = input_data.get("target_size_gb")

    if actual_size_gb is None or not isinstance(target_size_gb, int):
        return findings, "pass"

    if target_size_gb > actual_size_gb:
        findings.append(f"目标容量 {target_size_gb}GB 大于云盘实际容量 {actual_size_gb}GB，扩容目标有效。")
        return findings, "pass"

    findings.append(
        f"目标容量 {target_size_gb}GB 不大于云盘实际容量 {actual_size_gb}GB，当前不是有效扩容目标。"
    )
    return findings, "fail"


def _check_os_type_consistency(input_data: dict, instance: dict) -> tuple[list[str], str]:
    findings: list[str] = []
    input_os_type = input_data.get("os_type")
    actual_os_type = (instance.get("OSType") or "").lower()

    if not input_os_type or not actual_os_type:
        return findings, "pass"

    if input_os_type == actual_os_type:
        findings.append(f"输入 os_type={input_os_type} 与实例真实操作系统类型一致。")
        return findings, "pass"

    findings.append(
        f"输入 os_type={input_os_type} 与实例真实操作系统类型 {actual_os_type} 不一致。"
    )
    return findings, "fail"


def _check_current_size_consistency(input_data: dict, disk: dict) -> tuple[list[str], str]:
    findings: list[str] = []
    current_size_gb = input_data.get("current_size_gb")
    actual_size_gb = disk.get("Size")

    if current_size_gb is None or actual_size_gb is None:
        return findings, "pass"

    if current_size_gb == actual_size_gb:
        findings.append(f"输入 current_size_gb={current_size_gb} 与云盘实际容量一致。")
        return findings, "pass"

    findings.append(
        f"输入 current_size_gb={current_size_gb} 与云盘实际容量 {actual_size_gb}GB 不一致。"
    )

    target_size_gb = input_data.get("target_size_gb")
    if isinstance(target_size_gb, int) and target_size_gb <= actual_size_gb:
        findings.append(
            f"按云盘实际容量 {actual_size_gb}GB 判断，目标容量 {target_size_gb}GB 不是有效扩容目标。"
        )
        return findings, "fail"

    return findings, "warn"


def _check_dist_name_consistency(input_data: dict, instance: dict) -> tuple[list[str], str]:
    findings: list[str] = []
    dist_name = input_data.get("dist_name")
    if not dist_name:
        return findings, "pass"

    os_name = (instance.get("OSNameEn") or instance.get("OSName") or "").lower()
    normalized_dist = dist_name.lower()
    normalized_os_type = (instance.get("OSType") or "").lower()

    if normalized_dist in os_name:
        findings.append(f"输入 dist_name={dist_name} 与实例操作系统信息一致。")
        return findings, "pass"

    if normalized_os_type == "windows" and "windows" not in normalized_dist:
        findings.append(f"输入 dist_name={dist_name} 与实例真实操作系统不一致。")
        return findings, "fail"

    if normalized_os_type == "linux" and "windows" in normalized_dist:
        findings.append(f"输入 dist_name={dist_name} 与实例真实操作系统不一致。")
        return findings, "fail"

    findings.append(
        f"输入 dist_name={dist_name} 与实例返回的操作系统信息未完全匹配，当前实例系统为 {instance.get('OSNameEn') or instance.get('OSName')}。"
    )
    return findings, "warn"


def _check_os_layout_hints(input_data: dict) -> tuple[list[str], str]:
    findings: list[str] = []
    status = "pass"

    if input_data.get("filesystem_type") is not None:
        findings.append("已提供 filesystem_type，但当前版本尚未接入操作系统内探测，暂未校验其与真实文件系统是否一致。")
        status = "warn"

    if input_data.get("partition_scheme") is not None:
        findings.append("已提供 partition_scheme，但当前版本尚未接入分区探测，暂未校验其与真实分区格式是否一致。")
        status = "warn"

    if input_data.get("lvm_in_use") is not None:
        findings.append("已提供 lvm_in_use，但当前版本尚未接入卷管理探测，暂未校验其与真实 LVM 使用情况是否一致。")
        status = "warn"

    return findings, status


def _merge_status(current: str, new: str) -> str:
    if "fail" in {current, new}:
        return "fail"
    if "warn" in {current, new}:
        return "warn"
    return "pass"


def evaluate_online_expansion_support(
    input_data: dict,
    instance: dict,
    disk: dict,
) -> dict:
    findings: list[str] = []
    status = "pass"
    can_online_expand = True
    requires_shutdown = False
    requires_manual_review = False

    disk_category = disk.get("Category")
    if disk_category == "cloud":
        findings.append("目标磁盘类别为普通云盘 cloud，通常不支持在线扩容。")
        status = "fail"
        can_online_expand = False
        requires_shutdown = True
    elif disk_category:
        findings.append(f"目标磁盘类别为 {disk_category}，平台侧未见明显的在线扩容阻断。")
    else:
        findings.append("当前未获取到磁盘类别，无法准确判断平台是否支持在线扩容。")
        status = "warn"
        can_online_expand = False
        requires_manual_review = True

    instance_status = instance.get("Status")
    if instance_status == "Running":
        findings.append("实例当前处于 Running 状态，满足在线扩容的基础运行前提。")
    else:
        findings.append(f"实例当前状态为 {instance_status or 'unknown'}，不满足在线扩容的基础运行前提。")
        status = "fail"
        can_online_expand = False
        requires_shutdown = True

    disk_status = disk.get("Status")
    if disk_status == "In_use":
        findings.append("目标磁盘当前状态为 In_use，处于已挂载使用中。")
    else:
        findings.append(f"目标磁盘当前状态为 {disk_status or 'unknown'}，当前不适合作为在线扩容目标。")
        status = "fail"
        can_online_expand = False
        requires_shutdown = True

    target_size_gb = input_data.get("target_size_gb")
    actual_size_gb = disk.get("Size")
    if isinstance(target_size_gb, int) and actual_size_gb is not None:
        if target_size_gb > actual_size_gb:
            findings.append(f"目标容量 {target_size_gb}GB 大于当前磁盘容量 {actual_size_gb}GB。")
        else:
            findings.append(f"目标容量 {target_size_gb}GB 不大于当前磁盘容量 {actual_size_gb}GB，不是有效在线扩容目标。")
            status = "fail"
            can_online_expand = False

    operation_locks = disk.get("OperationLocks", {}).get("OperationLock", [])
    if operation_locks:
        lock_reasons = [item.get("LockReason", "unknown") for item in operation_locks]
        findings.append(f"目标磁盘存在操作锁：{', '.join(lock_reasons)}。")
        status = "fail"
        can_online_expand = False
        requires_manual_review = True
    else:
        findings.append("目标磁盘当前无操作锁。")

    disk_role = disk.get("Type")
    if disk_role == "system":
        findings.append("目标磁盘为系统盘，在线扩容可行性需要结合操作系统内扩容路径继续核实。")
        requires_shutdown = True if status != "fail" else requires_shutdown
        if status == "pass":
            status = "warn"
        requires_manual_review = True
    elif disk_role == "data":
        findings.append("目标磁盘为数据盘，通常比系统盘更适合在线扩容。")
    else:
        findings.append("当前未识别出目标磁盘的系统盘/数据盘角色，需人工核实。")
        can_online_expand = False
        requires_manual_review = True
        status = "warn" if status == "pass" else status

    if input_data.get("provider_constraints"):
        provider_findings, provider_status, provider_manual_review = _check_provider_constraints(input_data)
        findings.extend(provider_findings)
        status = _merge_status(status, provider_status)
        requires_manual_review = requires_manual_review or provider_manual_review

    if input_data.get("change_window"):
        findings.append("已提供 change_window，当前版本暂未自动校验窗口时间，仅保留为人工审核依据。")
        requires_manual_review = True
        status = "warn" if status == "pass" else status

    os_support_findings, os_support_status, os_manual_review = _check_os_online_expansion_hints(input_data)
    findings.extend(os_support_findings)
    status = _merge_status(status, os_support_status)
    requires_manual_review = requires_manual_review or os_manual_review

    if status == "fail":
        can_online_expand = False

    return {
        "status": status,
        "findings": findings,
        "can_online_expand": can_online_expand,
        "requires_shutdown": requires_shutdown,
        "requires_manual_review": requires_manual_review,
    }


def _check_provider_constraints(input_data: dict) -> tuple[list[str], str, bool]:
    findings: list[str] = []
    raw_text = input_data.get("provider_constraints", "")
    text = raw_text.lower()

    if not raw_text:
        return findings, "pass", False

    if "禁止" in raw_text or "不允许" in raw_text:
        findings.append("provider_constraints 明确包含禁止性限制，当前不应直接判定为支持在线扩容。")
        return findings, "fail", True

    if "人工审核" in raw_text or "审批" in raw_text:
        findings.append("provider_constraints 提示当前扩容场景需要人工审核或审批。")
        return findings, "warn", True

    if "变更窗口" in raw_text or "窗口" in text:
        findings.append("provider_constraints 提示扩容操作受变更窗口限制，需结合流程要求人工确认。")
        return findings, "warn", True

    findings.append("已提供 provider_constraints，但当前未识别出明确的阻断或放行条件，需人工复核。")
    return findings, "warn", True


def _check_os_online_expansion_hints(input_data: dict) -> tuple[list[str], str, bool]:
    findings: list[str] = []
    status = "pass"
    requires_manual_review = False

    os_type = input_data.get("os_type")
    dist_name = (input_data.get("dist_name") or "").lower()
    filesystem_type = (input_data.get("filesystem_type") or "").lower()
    partition_scheme = input_data.get("partition_scheme")
    lvm_in_use = input_data.get("lvm_in_use")

    if os_type == "windows":
        findings.append("目标实例为 Windows，系统盘在线扩容后的操作系统层处理需要重点确认。")
        requires_manual_review = True
        status = "warn"
        if filesystem_type and filesystem_type not in {"ntfs", "refs"}:
            findings.append(f"filesystem_type={filesystem_type} 与 Windows 常见在线扩容文件系统不匹配。")
            return findings, "fail", True
    elif os_type == "linux":
        findings.append("目标实例为 Linux，平台侧具备继续评估在线扩容路径的基础。")
        if filesystem_type in {"ntfs", "refs", "fat32"}:
            findings.append(f"filesystem_type={filesystem_type} 与 Linux 在线扩容场景明显不匹配。")
            return findings, "fail", True
    else:
        findings.append("当前未识别出目标实例的操作系统类型，无法判断在线扩容路径。")
        return findings, "fail", True

    if dist_name:
        if os_type == "linux" and "windows" in dist_name:
            findings.append(f"dist_name={dist_name} 与 Linux 场景不匹配。")
            return findings, "fail", True
        if os_type == "windows" and "windows" not in dist_name:
            findings.append(f"dist_name={dist_name} 与 Windows 场景不匹配。")
            return findings, "fail", True
        findings.append(f"已提供 dist_name={input_data.get('dist_name')}，可作为后续操作系统层扩容路径判断依据。")
    else:
        findings.append("未提供 dist_name，当前无法细化到具体发行版或 Windows 版本的在线扩容判断。")
        requires_manual_review = True
        status = "warn" if status == "pass" else status

    if filesystem_type:
        findings.append(f"已提供 filesystem_type={input_data.get('filesystem_type')}。")
    else:
        findings.append("未提供 filesystem_type，当前无法确认文件系统是否支持在线扩容后的在线扩展。")
        requires_manual_review = True
        status = "warn" if status == "pass" else status

    if partition_scheme:
        findings.append(f"已提供 partition_scheme={partition_scheme}。")
    else:
        findings.append("未提供 partition_scheme，当前无法确认后续分区扩展路径。")
        requires_manual_review = True
        status = "warn" if status == "pass" else status

    if lvm_in_use is not None:
        findings.append(f"已提供 lvm_in_use={lvm_in_use}，可用于判断卷管理复杂度。")
    else:
        findings.append("未提供 lvm_in_use，当前无法判断是否涉及 LVM 在线扩容路径。")
        requires_manual_review = True
        status = "warn" if status == "pass" else status

    return findings, status, requires_manual_review
