import base64
import json
import os
import re
import time

from alibabacloud_ecs20140526.client import Client as EcsClient
from alibabacloud_ecs20140526 import models as ecs_models
from alibabacloud_hbr20170908.client import Client as HbrClient
from alibabacloud_hbr20170908 import models as hbr_models
from alibabacloud_tea_openapi import models as open_api_models
from mcp.server.fastmcp import FastMCP

from .input_validation import validate_inputs
from .precheck_rules import evaluate_online_expansion_support, evaluate_optional_input_consistency

mcp = FastMCP("disk-expansion-precheck", json_response=True)


def get_instance_info(input_data: dict) -> dict:
    try:
        client = _create_ecs_client(input_data["region"])
        instance = _describe_instance(client, input_data)
        disk = _describe_disk(client, input_data)
    except ValueError as exc:
        return {
            "status": "fail",
            "findings": [str(exc)],
            "instance_context": _build_instance_context(input_data),
            "raw_instance_info": None,
            "raw_disk_info": None,
        }
    except Exception as exc:
        return {
            "status": "fail",
            "findings": [f"调用阿里云实例/磁盘查询接口失败：{exc}"],
            "instance_context": _build_instance_context(input_data),
            "raw_instance_info": None,
            "raw_disk_info": None,
        }

    findings = [
        f"已通过 DescribeInstances 查询到实例 {input_data['instance_id']}。",
        f"实例状态为 {instance.get('Status', 'unknown')}，实例规格为 {instance.get('InstanceType', 'unknown')}。",
    ]

    zone_id = instance.get("ZoneId") or input_data.get("zone_id")
    if zone_id:
        findings.append(f"实例所在可用区为 {zone_id}。")

    instance_name = instance.get("InstanceName") or input_data.get("instance_name")
    if instance_name:
        findings.append(f"实例名称为 {instance_name}。")

    disk_findings, disk_status = _build_disk_findings(instance, disk, input_data)
    findings.extend(disk_findings)
    optional_findings, optional_status = evaluate_optional_input_consistency(input_data, instance, disk)
    findings.extend(optional_findings)
    context_findings, missing_context, context_status = _build_context_findings(input_data)
    findings.extend(context_findings)

    final_status = _merge_check_status(disk_status, optional_status, context_status)

    return {
        "status": final_status,
        "findings": findings,
        "instance_context": {
            **_build_instance_context(input_data),
            "zone_id": zone_id,
            "instance_name": instance_name,
            "instance_type": instance.get("InstanceType"),
            "status": instance.get("Status"),
            "hostname": instance.get("HostName"),
            "disk_category": disk.get("Category"),
            "disk_status": disk.get("Status"),
            "disk_size_gb": disk.get("Size"),
            "actual_disk_role": _normalize_disk_role(disk.get("Type")),
        },
        "missing_context": missing_context,
        "raw_instance_info": instance,
        "raw_disk_info": disk,
    }


def get_online_expansion_support(input_data: dict, instance_info: dict) -> dict:
    if instance_info.get("status") == "fail":
        return {
            "status": "unknown",
            "findings": ["实例信息检查未通过，当前无法对在线扩容支持性形成可靠判断。"],
            "can_online_expand": False,
            "requires_shutdown": True,
            "requires_manual_review": True,
        }

    instance = instance_info.get("raw_instance_info")
    disk = instance_info.get("raw_disk_info")
    if not instance or not disk:
        return {
            "status": "unknown",
            "findings": ["缺少实例或磁盘详情，当前无法对在线扩容支持性形成可靠判断。"],
            "can_online_expand": False,
            "requires_shutdown": True,
            "requires_manual_review": True,
        }

    return evaluate_online_expansion_support(input_data, instance, disk)


def get_temporary_disk_risk(input_data: dict, instance_info: dict, online_expansion_support: dict) -> dict:
    if instance_info.get("status") == "fail":
        return {
            "status": "unknown",
            "findings": ["实例信息检查未通过，当前无法对临时磁盘与临时数据风险形成可靠判断。"],
            "has_temporary_storage": None,
            "has_application_data_on_temporary_storage": None,
            "business_risk": "unknown",
        }

    instance = instance_info.get("raw_instance_info")
    if not instance:
        return {
            "status": "unknown",
            "findings": ["缺少实例详情，当前无法对临时磁盘与临时数据风险形成可靠判断。"],
            "has_temporary_storage": None,
            "has_application_data_on_temporary_storage": None,
            "business_risk": "unknown",
        }

    if instance.get("Status") != "Running":
        return {
            "status": "unknown",
            "findings": [f"实例当前状态为 {instance.get('Status', 'unknown')}，云助手无法稳定完成临时磁盘探测。"],
            "has_temporary_storage": None,
            "has_application_data_on_temporary_storage": None,
            "business_risk": "unknown",
        }

    if input_data.get("os_type") != "linux":
        return {
            "status": "warn",
            "findings": ["当前版本只实现了 Linux 实例的临时磁盘探测，非 Linux 场景需人工确认。"],
            "has_temporary_storage": None,
            "has_application_data_on_temporary_storage": None,
            "business_risk": "unknown",
        }

    try:
        client = _create_ecs_client(input_data["region"])
        command_output = _run_linux_probe_command(client, input_data)
    except Exception as exc:
        return {
            "status": "warn",
            "findings": [f"云助手探测未成功执行：{exc}"],
            "has_temporary_storage": None,
            "has_application_data_on_temporary_storage": None,
            "business_risk": "unknown",
        }

    return _evaluate_temporary_disk_risk(input_data, instance, online_expansion_support, command_output)


def get_disk_layout_and_usage(input_data: dict, instance_info: dict) -> dict:
    if instance_info.get("status") == "fail":
        return {
            "status": "unknown",
            "findings": ["实例信息检查未通过，当前无法对磁盘分区与使用情况形成可靠判断。"],
            "layout_summary": None,
        }

    instance = instance_info.get("raw_instance_info")
    disk = instance_info.get("raw_disk_info")
    if not instance or not disk:
        return {
            "status": "unknown",
            "findings": ["缺少实例或磁盘详情，当前无法对磁盘分区与使用情况形成可靠判断。"],
            "layout_summary": None,
        }

    if instance.get("Status") != "Running":
        return {
            "status": "unknown",
            "findings": [f"实例当前状态为 {instance.get('Status', 'unknown')}，云助手无法稳定完成磁盘布局探测。"],
            "layout_summary": None,
        }

    if input_data.get("os_type") != "linux":
        return {
            "status": "warn",
            "findings": ["当前版本只实现了 Linux 实例的磁盘布局探测，非 Linux 场景需人工确认。"],
            "layout_summary": None,
        }

    try:
        client = _create_ecs_client(input_data["region"])
        command_output = _run_disk_layout_probe_command(client, input_data)
    except Exception as exc:
        return {
            "status": "warn",
            "findings": [f"磁盘布局探测未成功执行：{exc}"],
            "layout_summary": None,
        }

    return _evaluate_disk_layout_and_usage(input_data, disk, command_output)


def get_backup_management(input_data: dict, instance_info: dict) -> dict:
    if instance_info.get("status") == "fail":
        return {
            "status": "unknown",
            "findings": ["实例信息检查未通过，当前无法对备份管理情况形成可靠判断。"],
            "backup_summary": None,
        }

    instance = instance_info.get("raw_instance_info")
    disk = instance_info.get("raw_disk_info")
    if not instance or not disk:
        return {
            "status": "unknown",
            "findings": ["缺少实例或磁盘详情，当前无法对备份管理情况形成可靠判断。"],
            "backup_summary": None,
        }

    ecs_client = _create_ecs_client(input_data["region"])
    hbr_client = _create_hbr_client(input_data["region"])

    ecs_snapshots: list[dict] = []
    snapshot_usage: dict | None = None
    hbr_policies: list[dict] = []
    historical_snapshots: list[dict] = []
    latest_hbr_task: dict | None = None
    query_warnings: list[str] = []

    try:
        snapshot_request = ecs_models.DescribeSnapshotsRequest(
            region_id=input_data["region"],
            disk_id=input_data["disk_id"],
            page_size=10,
        )
        snapshot_response = ecs_client.describe_snapshots(snapshot_request)
        ecs_snapshots = snapshot_response.body.to_map().get("Snapshots", {}).get("Snapshot", [])
    except Exception as exc:
        query_warnings.append(f"ECS 快照查询失败：{exc}")

    try:
        usage_request = ecs_models.DescribeSnapshotsUsageRequest(region_id=input_data["region"])
        usage_response = ecs_client.describe_snapshots_usage(usage_request)
        snapshot_usage = usage_response.body.to_map()
    except Exception as exc:
        query_warnings.append(f"ECS 快照用量查询失败：{exc}")

    try:
        policies_response = hbr_client.describe_policies_v2(hbr_models.DescribePoliciesV2Request(max_results=20))
        hbr_policies = policies_response.body.to_map().get("Policies", [])
    except Exception as exc:
        query_warnings.append(f"HBR 备份策略查询失败：{exc}")

    vault_ids = _collect_hbr_vault_ids(hbr_policies)
    for vault_id in vault_ids[:3]:
        try:
            historical_request = hbr_models.SearchHistoricalSnapshotsRequest(
                limit=5,
                order="DESC",
                sort_by="CompleteTime",
                source_type="ECS_FILE",
                query=[
                    {"field": "VaultId", "value": vault_id, "operation": "MATCH_TERM"},
                    {"field": "InstanceId", "value": input_data["instance_id"], "operation": "MATCH_TERM"},
                ],
            )
            historical_response = hbr_client.search_historical_snapshots(historical_request)
            snapshots = historical_response.body.to_map().get("Snapshots", {}).get("Snapshot", [])
            historical_snapshots.extend(snapshots)
        except Exception as exc:
            query_warnings.append(f"HBR 历史快照查询失败（VaultId={vault_id}）：{exc}")

    latest_historical = _pick_latest_historical_snapshot(historical_snapshots)
    job_id = latest_historical.get("JobId") if latest_historical else None
    if job_id:
        try:
            task_response = hbr_client.describe_task(hbr_models.DescribeTaskRequest(task_id=job_id))
            latest_hbr_task = task_response.body.to_map()
        except Exception as exc:
            query_warnings.append(f"HBR 任务详情查询失败（TaskId={job_id}）：{exc}")

    return _evaluate_backup_management(
        input_data,
        ecs_snapshots,
        snapshot_usage,
        hbr_policies,
        historical_snapshots,
        latest_hbr_task,
        query_warnings,
    )


def get_resource_quota(input_data: dict, instance_info: dict) -> dict:
    if instance_info.get("status") == "fail":
        return {
            "status": "unknown",
            "findings": ["实例信息检查未通过，当前无法对资源配额剩余量形成可靠判断。"],
            "quota_summary": None,
        }

    instance = instance_info.get("raw_instance_info")
    disk = instance_info.get("raw_disk_info")
    if not instance or not disk:
        return {
            "status": "unknown",
            "findings": ["缺少实例或磁盘详情，当前无法对资源配额剩余量形成可靠判断。"],
            "quota_summary": None,
        }

    try:
        client = _create_ecs_client(input_data["region"])
        request = ecs_models.DescribeAccountAttributesRequest(
            region_id=input_data["region"],
            attribute_name=["max-instances", "supported-postpaid-instance-types"],
        )
        response = client.describe_account_attributes(request)
        account_attributes = response.body.to_map().get("AccountAttributeItems", {}).get("AccountAttributeItem", [])
    except Exception as exc:
        return {
            "status": "warn",
            "findings": [f"资源配额查询未成功执行：{exc}"],
            "quota_summary": None,
        }

    return _evaluate_resource_quota(input_data, instance, disk, account_attributes)


def _describe_instance(client: EcsClient, input_data: dict) -> dict:
    request = ecs_models.DescribeInstancesRequest(
        region_id=input_data["region"],
        instance_ids=json.dumps([input_data["instance_id"]]),
    )
    response = client.describe_instances(request)
    body = response.body.to_map()
    instances = body.get("Instances", {}).get("Instance", [])
    if not instances:
        raise ValueError(f"在地域 {input_data['region']} 中未查询到实例 {input_data['instance_id']}。")
    return instances[0]


def _describe_disk(client: EcsClient, input_data: dict) -> dict:
    request = ecs_models.DescribeDisksRequest(
        region_id=input_data["region"],
        instance_id=input_data["instance_id"],
        disk_ids=json.dumps([input_data["disk_id"]]),
    )
    response = client.describe_disks(request)
    body = response.body.to_map()
    disks = body.get("Disks", {}).get("Disk", [])
    if not disks:
        raise ValueError(
            f"在地域 {input_data['region']} 中未查询到磁盘 {input_data['disk_id']}，或该磁盘未挂载到实例 {input_data['instance_id']}。"
        )
    return disks[0]


def _run_linux_probe_command(client: EcsClient, input_data: dict) -> str:
    script = """#!/bin/sh
set -eu
echo '===FINDMNT==='
sudo -n findmnt -rn -o TARGET,SOURCE,FSTYPE,OPTIONS || true
echo '===DF==='
sudo -n df -hT || true
echo '===CANDIDATES==='
for p in /mnt /mnt/data /data/temp /temp /var/tmp /tmp; do
  if [ -e "$p" ]; then
    printf '%s|' "$p"
    sudo -n du -sh "$p" 2>/dev/null || true
  fi
done
"""
    request = ecs_models.RunCommandRequest(
        region_id=input_data["region"],
        type="RunShellScript",
        instance_id=[input_data["instance_id"]],
        timeout=60,
        username="yifan.li",
        content_encoding="Base64",
        command_content=base64.b64encode(script.encode("utf-8")).decode("ascii"),
    )
    run_response = client.run_command(request)
    invoke_id = run_response.body.invoke_id
    if not invoke_id:
        raise ValueError("云助手命令未返回 InvokeId。")

    for _ in range(12):
        time.sleep(2)
        invocation_request = ecs_models.DescribeInvocationsRequest(
            region_id=input_data["region"],
            invoke_id=invoke_id,
            instance_id=input_data["instance_id"],
        )
        invocation_response = client.describe_invocations(invocation_request)
        invocations = invocation_response.body.to_map().get("Invocations", {}).get("Invocation", [])
        if not invocations:
            continue
        invoke_status = invocations[0].get("InvokeStatus")
        if invoke_status == "Finished":
            return _fetch_invocation_output(client, input_data, invoke_id)
        if invoke_status in {"Failed", "Stopped", "PartialFailed", "Cancelled"}:
            raise ValueError(f"云助手命令执行失败，状态为 {invoke_status}。")

    raise TimeoutError("云助手命令执行超时。")


def _run_disk_layout_probe_command(client: EcsClient, input_data: dict) -> str:
    script = """#!/bin/sh
set -eu
echo '===LSBLK==='
sudo -n lsblk -o NAME,KNAME,TYPE,SIZE,FSTYPE,MOUNTPOINT,PKNAME,PTTYPE -J || true
echo '===FINDMNT==='
sudo -n findmnt -rn -o TARGET,SOURCE,FSTYPE,OPTIONS || true
echo '===DF==='
sudo -n df -hT || true
echo '===BLKID==='
sudo -n blkid || true
echo '===PVS==='
sudo -n pvs --noheadings --units g || true
echo '===VGS==='
sudo -n vgs --noheadings --units g || true
echo '===LVS==='
sudo -n lvs --noheadings --units g || true
echo '===MDSTAT==='
sudo -n cat /proc/mdstat || true
"""
    request = ecs_models.RunCommandRequest(
        region_id=input_data["region"],
        type="RunShellScript",
        instance_id=[input_data["instance_id"]],
        timeout=60,
        username="yifan.li",
        content_encoding="Base64",
        command_content=base64.b64encode(script.encode("utf-8")).decode("ascii"),
    )
    run_response = client.run_command(request)
    invoke_id = run_response.body.invoke_id
    if not invoke_id:
        raise ValueError("云助手命令未返回 InvokeId。")

    for _ in range(12):
        time.sleep(2)
        invocation_request = ecs_models.DescribeInvocationsRequest(
            region_id=input_data["region"],
            invoke_id=invoke_id,
            instance_id=input_data["instance_id"],
        )
        invocation_response = client.describe_invocations(invocation_request)
        invocations = invocation_response.body.to_map().get("Invocations", {}).get("Invocation", [])
        if not invocations:
            continue
        invoke_status = invocations[0].get("InvokeStatus")
        if invoke_status == "Finished":
            return _fetch_invocation_output(client, input_data, invoke_id)
        if invoke_status in {"Failed", "Stopped", "PartialFailed", "Cancelled"}:
            raise ValueError(f"云助手命令执行失败，状态为 {invoke_status}。")

    raise TimeoutError("云助手命令执行超时。")


def _fetch_invocation_output(client: EcsClient, input_data: dict, invoke_id: str) -> str:
    result_request = ecs_models.DescribeInvocationResultsRequest(
        region_id=input_data["region"],
        invoke_id=invoke_id,
        instance_id=input_data["instance_id"],
    )
    result_response = client.describe_invocation_results(result_request)
    body = result_response.body.to_map()
    results = body.get("Invocation", {}).get("InvocationResults", {}).get("InvocationResult", [])
    if not results:
        results = body.get("InvocationResults", {}).get("InvocationResult", [])
    if not results:
        raise ValueError("未获取到云助手命令输出结果。")

    record = results[0]
    output = record.get("Output", "")
    decoded_output = _decode_command_output(output)
    if decoded_output:
        return decoded_output

    invoke_record_status = record.get("InvokeRecordStatus") or record.get("InvocationStatus")
    exit_code = record.get("ExitCode")
    error_info = record.get("ErrorInfo")
    username = record.get("Username")
    dropped = record.get("Dropped")
    raise ValueError(
        "未获取到云助手命令输出结果。"
        f" InvokeRecordStatus={invoke_record_status}, ExitCode={exit_code}, "
        f"Username={username}, Dropped={dropped}, ErrorInfo={error_info}"
    )


def _decode_command_output(output: str) -> str:
    if not output:
        return ""
    try:
        return base64.b64decode(output).decode("utf-8", errors="replace")
    except Exception:
        return output


def _build_disk_findings(instance: dict, disk: dict, input_data: dict) -> tuple[list[str], str]:
    findings = []
    status = "pass"
    actual_disk_role = _normalize_disk_role(disk.get("Type"))

    if actual_disk_role:
        findings.append(f"已通过 DescribeDisks 查询到磁盘 {input_data['disk_id']}，实际磁盘角色为 {actual_disk_role}。")
    else:
        findings.append(f"已通过 DescribeDisks 查询到磁盘 {input_data['disk_id']}，但当前返回中未识别出明确的磁盘角色。")
        status = "warn"

    if disk.get("InstanceId") == input_data["instance_id"]:
        findings.append(f"已确认磁盘 {input_data['disk_id']} 当前挂载在实例 {input_data['instance_id']} 上。")
    else:
        findings.append(f"磁盘 {input_data['disk_id']} 当前未挂载到目标实例 {input_data['instance_id']}，需要人工确认。")
        status = "fail"

    if actual_disk_role and actual_disk_role != input_data["disk_role"]:
        findings.append(f"输入 disk_role={input_data['disk_role']} 与阿里云返回的实际磁盘角色 {actual_disk_role} 不一致。")
        status = "fail"

    if instance.get("Status") != "Running" and status == "pass":
        findings.append(f"实例当前状态为 {instance.get('Status', 'unknown')}，后续在线扩容支持性需结合实例状态继续判断。")
        status = "warn"

    return findings, status


def _build_context_findings(input_data: dict) -> tuple[list[str], list[str], str]:
    findings = []
    missing_context = []
    status = "pass"

    service_criticality = input_data.get("service_criticality")
    asset_information = input_data.get("asset_information")
    topology_information = input_data.get("topology_information")

    if service_criticality:
        findings.append(f"已提供业务重要级别：{service_criticality}。")
    else:
        findings.append("未提供业务重要级别，当前无法明确实例的业务影响等级。")
        missing_context.append("service_criticality")
        status = "warn"

    if asset_information:
        findings.append("已提供资产属性补充信息，可用于后续风险评估。")
    else:
        findings.append("未提供资产属性补充信息，当前无法确认环境类型、应用名称或负责人等业务上下文。")
        missing_context.append("asset_information")
        status = "warn"

    if topology_information:
        findings.append("已提供拓扑补充信息，可用于识别实例在应用资源拓扑中的位置。")
    else:
        findings.append("未提供拓扑补充信息，当前无法判断该实例是否为单点实例或集群节点。")
        missing_context.append("topology_information")
        status = "warn"

    return findings, missing_context, status


def _evaluate_temporary_disk_risk(input_data: dict, instance: dict, online_expansion_support: dict, command_output: str) -> dict:
    findings: list[str] = []
    status = "pass"
    has_temporary_storage = False
    has_application_data = False
    business_risk = "low"

    candidate_paths = ["/mnt", "/mnt/data", "/data/temp", "/temp", "/var/tmp", "/tmp"]
    if input_data.get("temporary_disk_hints"):
        candidate_paths.extend(path.strip() for path in input_data["temporary_disk_hints"].split(",") if path.strip())

    matched_paths = sorted({path for path in candidate_paths if path in command_output})
    if matched_paths:
        has_temporary_storage = True
        findings.append(f"在实例内发现临时路径线索：{', '.join(matched_paths)}。")
        status = "warn"
    else:
        findings.append("当前未在预设临时路径中发现明显的临时磁盘或易失性存储线索。")

    risky_keywords = ["upload", "cache", "log", "tmp", "temp", "redis", "kafka", "mysql", "postgres"]
    if matched_paths and any(keyword in command_output.lower() for keyword in risky_keywords):
        has_application_data = True
        findings.append("临时路径输出中出现应用、日志、缓存或中间件相关线索，需重点确认是否承载业务数据。")
        status = "warn"
        business_risk = "high" if online_expansion_support.get("requires_shutdown") else "medium"
    elif matched_paths:
        findings.append("已发现临时路径，但尚未从当前输出中确认存在明确的业务数据线索。")
        business_risk = "medium" if online_expansion_support.get("requires_shutdown") else "low"

    if online_expansion_support.get("requires_shutdown") and has_temporary_storage:
        findings.append("后续扩容过程可能涉及停机或重启，临时磁盘中的易失性数据存在丢失风险。")
        business_risk = "high"
        status = "warn"

    findings.append("当前临时磁盘风险判断基于云助手只读探测结果，仍建议在实施前人工复核挂载路径和数据用途。")

    return {
        "status": status,
        "findings": findings,
        "has_temporary_storage": has_temporary_storage,
        "has_application_data_on_temporary_storage": has_application_data,
        "business_risk": business_risk,
        "probe_output": command_output,
    }


def _evaluate_disk_layout_and_usage(input_data: dict, disk: dict, command_output: str) -> dict:
    findings: list[str] = []
    status = "pass"
    lsblk_nodes = _load_lsblk_nodes(_extract_probe_section(command_output, "LSBLK"))
    findmnt_entries = _parse_findmnt_entries(_extract_probe_section(command_output, "FINDMNT"))
    df_lines = _extract_probe_section(command_output, "DF").splitlines()
    pvs_lines = _non_empty_section_lines(_extract_probe_section(command_output, "PVS"))
    vgs_lines = _non_empty_section_lines(_extract_probe_section(command_output, "VGS"))
    lvs_lines = _non_empty_section_lines(_extract_probe_section(command_output, "LVS"))
    mdstat_section = _extract_probe_section(command_output, "MDSTAT")

    layout_summary: dict = {
        "disk_device": disk.get("Device"),
        "filesystem_type": input_data.get("filesystem_type"),
        "mount_points": [],
        "uses_lvm": False,
        "partition_scheme": input_data.get("partition_scheme"),
        "partition_count": 0,
        "partition_details": [],
        "storage_management": "unknown",
        "uses_raid": False,
        "filesystem_usage": [],
        "capacity_constraints": [],
        "complex_layout": False,
        "expansion_path": "unknown",
        "requires_manual_review": False,
    }

    disk_device = disk.get("Device")
    related_nodes = _find_related_lsblk_nodes(lsblk_nodes, disk_device)
    partition_nodes = [node for node in related_nodes if node.get("type") == "part"]
    layout_summary["partition_count"] = len(partition_nodes)
    layout_summary["partition_details"] = [
        {
            "device": node.get("path"),
            "filesystem_type": node.get("fstype"),
            "mount_point": node.get("mountpoint"),
            "parent_device": node.get("parent_path"),
        }
        for node in partition_nodes
    ]

    if disk_device and related_nodes:
        detected_paths = sorted({node["path"] for node in related_nodes if node.get("path")})
        findings.append(
            f"已在实例内探测到目标磁盘对应的设备链路，云侧设备 {disk_device} 在系统内对应 "
            f"{', '.join(detected_paths)}。"
        )
    elif disk_device:
        findings.append(f"云侧返回的目标磁盘设备路径为 {disk_device}，但当前命令输出中未直接匹配到该路径。")
        status = "warn"
        layout_summary["requires_manual_review"] = True
    else:
        findings.append("云侧未返回明确的目标磁盘设备路径，需人工确认。")
        status = "warn"
        layout_summary["requires_manual_review"] = True

    if layout_summary["partition_count"] == 0:
        findings.append("当前未识别到目标磁盘的分区记录，扩容路径可能是裸盘文件系统或需进一步确认。")
    else:
        findings.append(f"已识别到目标磁盘包含 {layout_summary['partition_count']} 个分区。")

    related_sources = _collect_related_sources(disk_device, related_nodes)
    for entry in findmnt_entries:
        if _is_source_related_to_disk(entry["source"], related_sources, disk_device):
            layout_summary["mount_points"].append(
                {
                    "mount_point": entry["mount_point"],
                    "source": entry["source"],
                    "filesystem_type": entry["filesystem_type"],
                }
            )

    if layout_summary["mount_points"]:
        mount_targets = ", ".join(item["mount_point"] for item in layout_summary["mount_points"])
        findings.append(f"已识别到目标磁盘相关挂载点：{mount_targets}。")
    else:
        findings.append("当前未从目标磁盘相关的 findmnt 输出中识别到明确挂载点记录。")
        status = "warn"
        layout_summary["requires_manual_review"] = True

    if any("LVM2_member" == (node.get("fstype") or "") for node in related_nodes) or pvs_lines or vgs_lines or lvs_lines:
        layout_summary["uses_lvm"] = True
        layout_summary["storage_management"] = "lvm"
        findings.append("已识别到 LVM 相关线索，后续扩容路径可能涉及 PV/VG/LV 处理。")
        status = "warn"
    else:
        findings.append("当前未识别到明显的 LVM 使用线索。")

    if _detect_raid_usage(related_nodes, mdstat_section):
        layout_summary["uses_raid"] = True
        layout_summary["storage_management"] = "raid" if layout_summary["storage_management"] == "unknown" else "lvm+raid"
        findings.append("已识别到 RAID 相关线索，后续扩容路径需要结合阵列层处理。")
        status = "warn"
        layout_summary["requires_manual_review"] = True
    elif layout_summary["storage_management"] == "unknown":
        layout_summary["storage_management"] = "partition" if partition_nodes else "filesystem_on_disk"

    filesystem_types = [item["filesystem_type"] for item in layout_summary["mount_points"] if item.get("filesystem_type")]
    if filesystem_types:
        detected_fs = filesystem_types[0]
        layout_summary["filesystem_type"] = layout_summary["filesystem_type"] or detected_fs
        findings.append(f"当前命令输出中识别到目标磁盘文件系统为 {detected_fs}。")
    elif layout_summary["filesystem_type"] is None:
        findings.append("当前尚未明确识别出目标磁盘文件系统类型。")
        status = "warn"

    partition_scheme = _infer_partition_scheme(related_nodes)
    if partition_scheme:
        layout_summary["partition_scheme"] = layout_summary["partition_scheme"] or partition_scheme
        findings.append(f"已通过 lsblk 识别到目标磁盘分区格式为 {partition_scheme}。")
    elif any((node.get("fstype") or "") == "vfat" for node in related_nodes) and layout_summary["partition_scheme"] is None:
        findings.append("实例内存在 EFI 分区线索，但当前仍未明确识别目标磁盘的分区格式。")
        status = "warn"
        layout_summary["requires_manual_review"] = True

    related_df_lines = [line for line in df_lines if any(item["mount_point"] in line for item in layout_summary["mount_points"])]
    layout_summary["filesystem_usage"] = _parse_df_usage(related_df_lines, layout_summary["mount_points"])
    if any("100%" in line for line in related_df_lines):
        findings.append("磁盘使用率需结合 df 输出进一步人工确认，当前已获取容量使用信息。")
        layout_summary["capacity_constraints"].append("存在文件系统使用率 100% 的挂载点。")
        layout_summary["requires_manual_review"] = True
        status = "warn"
    else:
        findings.append("已通过 df 输出获取文件系统容量使用信息。")

    if not layout_summary["filesystem_usage"]:
        findings.append("尚未从 df 输出中提取到目标磁盘相关的容量使用记录。")
        layout_summary["requires_manual_review"] = True
        status = "warn"

    layout_summary["capacity_constraints"].extend(
        _build_capacity_constraints(layout_summary["filesystem_usage"], layout_summary["partition_count"], layout_summary["uses_lvm"])
    )
    if layout_summary["capacity_constraints"]:
        findings.append("已识别到需要关注的容量约束：" + "；".join(layout_summary["capacity_constraints"]))
        status = "warn"

    complex_layout_reasons = _detect_complex_layout_reasons(
        layout_summary["partition_count"],
        layout_summary["uses_lvm"],
        layout_summary["uses_raid"],
        layout_summary["mount_points"],
        related_nodes,
    )
    if complex_layout_reasons:
        layout_summary["complex_layout"] = True
        layout_summary["requires_manual_review"] = True
        findings.append("目标磁盘存在较复杂的分区或存储布局：" + "；".join(complex_layout_reasons))
        status = "warn"
    else:
        findings.append("当前未识别到明显的复杂分区布局。")

    layout_summary["expansion_path"] = _build_expansion_path(layout_summary)
    findings.append(f"当前推断的扩容处理路径为：{layout_summary['expansion_path']}。")

    return {
        "status": status,
        "findings": findings,
        "layout_summary": layout_summary,
        "probe_output": command_output,
    }


def _extract_probe_section(command_output: str, section_name: str) -> str:
    marker = f"==={section_name}==="
    if marker not in command_output:
        return ""
    tail = command_output.split(marker, 1)[1]
    next_marker = re.search(r"\n===[A-Z]+===\n?", tail)
    if next_marker:
        return tail[:next_marker.start()].strip()
    return tail.strip()


def _load_lsblk_nodes(lsblk_section: str) -> list[dict]:
    if not lsblk_section:
        return []
    try:
        payload = json.loads(lsblk_section)
    except json.JSONDecodeError:
        return []

    nodes: list[dict] = []

    def walk(blockdevices: list[dict], parent_path: str | None = None) -> None:
        for device in blockdevices:
            path = _device_path_from_lsblk_node(device)
            nodes.append(
                {
                    "path": path,
                    "type": device.get("type"),
                    "fstype": device.get("fstype"),
                    "mountpoint": device.get("mountpoint"),
                    "pkname": device.get("pkname"),
                    "parent_path": parent_path,
                    "pttype": device.get("pttype"),
                }
            )
            walk(device.get("children", []), path)

    walk(payload.get("blockdevices", []))
    return nodes


def _device_path_from_lsblk_node(device: dict) -> str | None:
    kname = device.get("kname")
    name = device.get("name")
    if kname:
        return f"/dev/{kname}"
    if name:
        return f"/dev/{name}"
    return None


def _parse_findmnt_entries(findmnt_section: str) -> list[dict]:
    entries: list[dict] = []
    for line in findmnt_section.splitlines():
        parts = line.split()
        if len(parts) < 3:
            continue
        entries.append(
            {
                "mount_point": parts[0],
                "source": parts[1],
                "filesystem_type": parts[2],
                "options": " ".join(parts[3:]) if len(parts) > 3 else "",
            }
        )
    return entries


def _non_empty_section_lines(section: str) -> list[str]:
    return [line.strip() for line in section.splitlines() if line.strip()]


def _find_related_lsblk_nodes(lsblk_nodes: list[dict], disk_device: str | None) -> list[dict]:
    if not disk_device:
        return []

    target_device = _normalize_device_path(disk_device)
    related_nodes: list[dict] = []
    for node in lsblk_nodes:
        node_path = _normalize_device_path(node.get("path"))
        parent_path = _normalize_device_path(node.get("parent_path"))
        pkname_path = _normalize_device_path(f"/dev/{node['pkname']}") if node.get("pkname") else None
        if node_path == target_device or parent_path == target_device or _normalize_device_path(_base_block_device(pkname_path)) == target_device:
            related_nodes.append(node)
    return related_nodes


def _collect_related_sources(disk_device: str | None, related_nodes: list[dict]) -> set[str]:
    related_sources = set()
    if disk_device:
        related_sources.add(_normalize_device_path(disk_device))
        related_sources.add(_normalize_device_path(_base_block_device(disk_device)))

    for node in related_nodes:
        if node.get("path"):
            related_sources.add(_normalize_device_path(node["path"]))
            related_sources.add(_normalize_device_path(_base_block_device(node["path"])))
        if node.get("parent_path"):
            related_sources.add(_normalize_device_path(node["parent_path"]))
    return {item for item in related_sources if item}


def _is_source_related_to_disk(source: str, related_sources: set[str], disk_device: str | None) -> bool:
    normalized_source = _normalize_device_path(source)
    normalized_base = _normalize_device_path(_base_block_device(source))
    normalized_disk = _normalize_device_path(disk_device)
    return normalized_source in related_sources or normalized_base in related_sources or normalized_base == normalized_disk


def _normalize_device_path(path: str | None) -> str | None:
    if not path:
        return None
    if path.startswith("/dev/xvd"):
        return "/dev/vd" + path[len("/dev/xvd"):]
    return path


def _base_block_device(path: str | None) -> str | None:
    if not path:
        return None
    nvme_match = re.match(r"^(/dev/nvme\d+n\d+)p\d+$", path)
    if nvme_match:
        return nvme_match.group(1)
    standard_match = re.match(r"^(/dev/[a-z]+)\d+$", path)
    if standard_match:
        return standard_match.group(1)
    return path


def _infer_partition_scheme(related_nodes: list[dict]) -> str | None:
    for node in related_nodes:
        pttype = (node.get("pttype") or "").lower()
        if pttype == "gpt":
            return "GPT"
        if pttype == "dos":
            return "MBR"
    return None


def _detect_raid_usage(related_nodes: list[dict], mdstat_section: str) -> bool:
    if "active raid" in mdstat_section.lower():
        return True
    return any((node.get("type") or "").startswith("raid") for node in related_nodes)


def _parse_df_usage(df_lines: list[str], mount_points: list[dict]) -> list[dict]:
    usage_records: list[dict] = []
    target_mounts = {item["mount_point"]: item for item in mount_points}
    for line in df_lines:
        parts = line.split()
        if len(parts) < 7:
            continue
        mount_point = parts[-1]
        if mount_point not in target_mounts:
            continue
        usage_records.append(
            {
                "filesystem": parts[0],
                "filesystem_type": parts[1],
                "size": parts[2],
                "used": parts[3],
                "available": parts[4],
                "use_percent": parts[5],
                "mount_point": mount_point,
            }
        )
    return usage_records


def _build_capacity_constraints(filesystem_usage: list[dict], partition_count: int, uses_lvm: bool) -> list[str]:
    constraints: list[str] = []
    if partition_count > 0:
        constraints.append("后续扩容需同时确认分区层是否需要调整。")
    if uses_lvm:
        constraints.append("后续扩容需同时确认 PV/VG/LV 链路容量是否可扩展。")

    for usage in filesystem_usage:
        use_percent = usage.get("use_percent", "")
        try:
            percent_value = int(use_percent.rstrip("%"))
        except ValueError:
            continue
        if percent_value >= 80:
            constraints.append(f"{usage['mount_point']} 当前使用率为 {use_percent}，扩容实施前应预留回退与校验空间。")
    return constraints


def _detect_complex_layout_reasons(
    partition_count: int,
    uses_lvm: bool,
    uses_raid: bool,
    mount_points: list[dict],
    related_nodes: list[dict],
) -> list[str]:
    reasons: list[str] = []
    if partition_count >= 3:
        reasons.append(f"该磁盘存在 {partition_count} 个分区")
    if uses_lvm:
        reasons.append("磁盘被纳入 LVM 管理")
    if uses_raid:
        reasons.append("磁盘存在 RAID 线索")
    if len(mount_points) >= 2:
        reasons.append("同一磁盘承载多个挂载点")
    if any((node.get("fstype") or "") == "vfat" for node in related_nodes):
        reasons.append("磁盘包含 EFI 或引导相关分区")
    return reasons


def _build_expansion_path(layout_summary: dict) -> str:
    if layout_summary["uses_raid"]:
        return "先确认 RAID 层处理，再扩分区或逻辑卷，最后扩文件系统"
    if layout_summary["uses_lvm"]:
        return "扩磁盘后检查分区，再扩 PV/VG/LV，最后扩文件系统"
    if layout_summary["partition_count"] > 0:
        return "扩磁盘后调整目标分区，再扩对应文件系统"
    if layout_summary["filesystem_type"]:
        return "扩磁盘后直接扩目标文件系统"
    return "扩容路径暂未明确，需人工确认"


def _evaluate_backup_management(
    input_data: dict,
    ecs_snapshots: list[dict],
    snapshot_usage: dict | None,
    hbr_policies: list[dict],
    historical_snapshots: list[dict],
    latest_hbr_task: dict | None,
    query_warnings: list[str],
) -> dict:
    findings: list[str] = []
    status = "pass"

    latest_ecs_snapshot = _pick_latest_ecs_snapshot(ecs_snapshots)
    latest_historical = _pick_latest_historical_snapshot(historical_snapshots)
    active_policies = [policy for policy in hbr_policies if policy.get("BusinessStatus") == "ACTIVE"]
    bound_active_policies = [policy for policy in active_policies if int(policy.get("PolicyBindingCount") or 0) > 0]
    valid_vault_ids = _collect_hbr_vault_ids(bound_active_policies)
    matched_policy = _match_expected_backup_policy(input_data.get("expected_backup_policy"), hbr_policies)

    available_mechanisms: list[str] = []
    if ecs_snapshots:
        available_mechanisms.append("ecs_snapshot")
    if bound_active_policies:
        available_mechanisms.append("hbr_policy")
    if historical_snapshots:
        available_mechanisms.append("hbr_historical_snapshot")

    latest_backup_source = None
    latest_backup_time = None
    latest_backup_status = None
    if latest_ecs_snapshot and _snapshot_time_value(latest_ecs_snapshot.get("CreationTime")) >= _historical_time_value(latest_historical):
        latest_backup_source = "ecs_snapshot"
        latest_backup_time = latest_ecs_snapshot.get("CreationTime")
        latest_backup_status = latest_ecs_snapshot.get("Status")
    elif latest_historical:
        latest_backup_source = "hbr_historical_snapshot"
        latest_backup_time = _format_unix_timestamp(
            latest_historical.get("CompleteTime") or latest_historical.get("UpdatedTime") or latest_historical.get("CreatedTime")
        )
        latest_backup_status = latest_historical.get("Status")

    has_rollback_basis = bool(latest_ecs_snapshot or latest_historical)
    backup_scope_covered = has_rollback_basis or bool(bound_active_policies)
    latest_task_status = _extract_hbr_task_status(latest_hbr_task)
    task_duration_seconds = _extract_hbr_task_duration(latest_hbr_task)

    if ecs_snapshots:
        findings.append(f"已查询到目标磁盘关联的 ECS 快照，共 {len(ecs_snapshots)} 个。")
    else:
        findings.append("当前未查询到目标磁盘关联的 ECS 快照。")
        if not historical_snapshots:
            status = "warn"

    if snapshot_usage:
        findings.append(
            f"当前地域快照使用概况：快照数量 {snapshot_usage.get('SnapshotCount', 'unknown')}，"
            f"快照容量 {snapshot_usage.get('SnapshotSize', 'unknown')} 字节。"
        )

    if active_policies:
        findings.append(f"已识别到 {len(active_policies)} 个处于 ACTIVE 状态的 HBR 备份策略。")
        if bound_active_policies:
            findings.append(f"其中有 {len(bound_active_policies)} 个策略已绑定受保护资源。")
        else:
            findings.append("当前 ACTIVE 的 HBR 策略未绑定受保护资源，尚不能证明目标实例或磁盘已纳入备份覆盖。")
            status = "warn"
    elif hbr_policies:
        findings.append("已查询到 HBR 备份策略，但当前没有处于 ACTIVE 状态的策略。")
        status = "warn"
    else:
        findings.append("当前未查询到可用的 HBR 备份策略。")
        status = "warn"

    if active_policies and not valid_vault_ids:
        findings.append("当前 HBR 策略规则中未返回有效 VaultId，无法继续核实历史备份快照。")
        status = "warn"

    if matched_policy:
        findings.append(f"已匹配到期望的备份策略 {matched_policy.get('PolicyName', matched_policy.get('PolicyId', 'unknown'))}。")
    elif input_data.get("expected_backup_policy"):
        findings.append("已提供期望备份策略，但当前返回中未匹配到对应的 HBR 策略。")
        status = "warn"

    if latest_backup_time:
        findings.append(f"最近一次识别到的备份时间为 {latest_backup_time}，来源为 {latest_backup_source}。")
    else:
        findings.append("当前未识别到最近一次成功备份时间。")
        status = "warn"

    if latest_backup_status:
        findings.append(f"最近一次备份状态为 {latest_backup_status}。")
        if str(latest_backup_status).lower() not in {"accomplished", "successful", "completed", "finish", "finished"}:
            status = "warn"
    else:
        findings.append("当前未识别到最近一次备份状态。")
        status = "warn"

    if latest_task_status:
        findings.append(f"最近一次 HBR 任务状态为 {latest_task_status}。")
        if latest_task_status not in {"completed", "successful"}:
            status = "warn"
    elif latest_hbr_task is not None:
        findings.append("已查询到 HBR 任务详情，但未明确识别出任务状态。")
        status = "warn"

    if task_duration_seconds is not None:
        findings.append(f"最近一次 HBR 任务耗时约 {task_duration_seconds} 秒。")
        if input_data.get("change_window"):
            findings.append("当前已记录变更窗口信息，任务耗时需结合实际变更窗口人工确认。")

    if not has_rollback_basis:
        findings.append("当前缺少已完成的快照或历史备份记录，扩容失败后的回退基础不足。")
        status = "fail"
    elif not backup_scope_covered:
        findings.append("当前未确认实例或磁盘已处于有效备份覆盖范围内。")
        status = "warn"

    if query_warnings:
        findings.extend(query_warnings)
        if status == "pass":
            status = "warn"

    backup_summary = {
        "has_rollback_basis": has_rollback_basis,
        "backup_scope_covered": backup_scope_covered,
        "available_mechanisms": available_mechanisms,
        "latest_backup_time": latest_backup_time,
        "latest_backup_source": latest_backup_source,
        "latest_backup_status": latest_backup_status,
        "latest_task_status": latest_task_status,
        "latest_task_duration_seconds": task_duration_seconds,
        "ecs_snapshot_count": len(ecs_snapshots),
        "active_hbr_policy_count": len(active_policies),
        "bound_active_hbr_policy_count": len(bound_active_policies),
        "valid_hbr_vault_count": len(valid_vault_ids),
        "matched_expected_policy": matched_policy.get("PolicyName") if matched_policy else None,
    }

    return {
        "status": status,
        "findings": findings,
        "backup_summary": backup_summary,
        "raw_ecs_snapshots": ecs_snapshots,
        "raw_snapshot_usage": snapshot_usage,
        "raw_hbr_policies": hbr_policies,
        "raw_historical_snapshots": historical_snapshots,
        "raw_latest_hbr_task": latest_hbr_task,
    }


def _pick_latest_ecs_snapshot(ecs_snapshots: list[dict]) -> dict | None:
    if not ecs_snapshots:
        return None
    return max(ecs_snapshots, key=lambda item: _snapshot_time_value(item.get("CreationTime")))


def _pick_latest_historical_snapshot(historical_snapshots: list[dict]) -> dict | None:
    if not historical_snapshots:
        return None
    return max(
        historical_snapshots,
        key=lambda item: max(int(item.get("CompleteTime") or 0), int(item.get("UpdatedTime") or 0), int(item.get("CreatedTime") or 0)),
    )


def _snapshot_time_value(timestamp: str | None) -> int:
    if not timestamp:
        return 0
    return int(time.mktime(time.strptime(timestamp, "%Y-%m-%dT%H:%MZ")))


def _historical_time_value(snapshot: dict | None) -> int:
    if not snapshot:
        return 0
    return int(snapshot.get("CompleteTime") or snapshot.get("UpdatedTime") or snapshot.get("CreatedTime") or 0)


def _format_unix_timestamp(timestamp: int | None) -> str | None:
    if not timestamp:
        return None
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(timestamp)))


def _match_expected_backup_policy(expected_backup_policy: str | None, hbr_policies: list[dict]) -> dict | None:
    if not expected_backup_policy:
        return None
    expected = expected_backup_policy.strip().lower()
    for policy in hbr_policies:
        if expected in str(policy.get("PolicyName", "")).lower() or expected in str(policy.get("PolicyId", "")).lower():
            return policy
    return None


def _extract_hbr_task_status(latest_hbr_task: dict | None) -> str | None:
    if not latest_hbr_task:
        return None
    description = str(latest_hbr_task.get("Description") or "").strip().lower()
    result = str(latest_hbr_task.get("Result") or "").strip().lower()
    if description:
        return description
    if result:
        return result
    return None


def _extract_hbr_task_duration(latest_hbr_task: dict | None) -> int | None:
    if not latest_hbr_task:
        return None
    created_time = latest_hbr_task.get("CreatedTime")
    completed_time = latest_hbr_task.get("CompletedTime")
    if created_time and completed_time:
        return int(completed_time) - int(created_time)
    return None


def _collect_hbr_vault_ids(hbr_policies: list[dict]) -> list[str]:
    vault_ids: set[str] = set()
    for policy in hbr_policies:
        for rule in policy.get("Rules", []):
            vault_id = rule.get("VaultId")
            if vault_id:
                vault_ids.add(vault_id)
    return sorted(vault_ids)


def _evaluate_resource_quota(input_data: dict, instance: dict, disk: dict, account_attributes: list[dict]) -> dict:
    findings: list[str] = []
    status = "pass"

    attributes_map = _account_attributes_to_map(account_attributes)
    supported_postpaid_types = attributes_map.get("supported-postpaid-instance-types", [])
    max_instances_values = attributes_map.get("max-instances", [])
    instance_type = instance.get("InstanceType")
    charge_type = instance.get("InstanceChargeType")

    if account_attributes:
        findings.append(f"已查询到账户属性，共返回 {len(account_attributes)} 项与配额/规格相关的账号能力信息。")
    else:
        findings.append("当前未查询到账户属性，暂时无法确认账号级配额限制。")
        status = "warn"

    if charge_type == "PostPaid":
        if instance_type in supported_postpaid_types:
            findings.append(f"当前实例规格 {instance_type} 位于账号支持的后付费实例规格范围内。")
        elif supported_postpaid_types:
            findings.append(f"当前实例规格 {instance_type} 未出现在返回的后付费实例规格列表中，需人工确认账号侧可用性。")
            status = "warn"
        else:
            findings.append("当前未从账号属性中拿到后付费实例规格列表，无法细化判断规格可用性。")
            status = "warn"
    else:
        findings.append(f"当前实例计费类型为 {charge_type or 'unknown'}，账号属性中的后付费规格列表仅可作为参考。")
        status = "warn"

    if max_instances_values:
        findings.append(f"账号属性返回的 max-instances 值为 {', '.join(max_instances_values)}。")
    else:
        findings.append("当前接口未返回 max-instances 属性值，无法确认账号实例数量上限。")
        status = "warn"

    if input_data.get("quota_scope_hint"):
        findings.append(f"已提供配额范围提示：{input_data['quota_scope_hint']}。")
    else:
        findings.append("未提供 quota_scope_hint，当前无法细化到特定资源池、项目或预算范围。")
        status = "warn"

    if disk.get("Category") in {"cloud_ssd", "cloud_essd", "cloud_efficiency"}:
        findings.append(f"目标磁盘类型为 {disk.get('Category')}，从产品类别看未见明显的扩容资源类型阻断。")
    else:
        findings.append(f"目标磁盘类型为 {disk.get('Category', 'unknown')}，当前需结合资源可售状态进一步人工确认。")
        status = "warn"

    findings.append("当前版本的资源配额检查基于账号属性能力做保守判断，尚不能直接确认地域级磁盘容量配额是否充足。")
    if status == "pass":
        status = "warn"

    quota_summary = {
        "account_attribute_count": len(account_attributes),
        "supported_postpaid_instance_types_count": len(supported_postpaid_types),
        "instance_type_supported": instance_type in supported_postpaid_types if supported_postpaid_types else None,
        "max_instances_values": max_instances_values,
        "quota_scope_hint": input_data.get("quota_scope_hint"),
        "can_confirm_region_disk_quota": False,
    }

    return {
        "status": status,
        "findings": findings,
        "quota_summary": quota_summary,
        "raw_account_attributes": account_attributes,
    }


def _account_attributes_to_map(account_attributes: list[dict]) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {}
    for item in account_attributes:
        name = item.get("AttributeName")
        values = [
            value_item.get("Value")
            for value_item in item.get("AttributeValues", {}).get("ValueItem", [])
            if value_item.get("Value") is not None
        ]
        if name:
            result[name] = values
    return result


def _merge_check_status(*statuses: str) -> str:
    if "fail" in statuses:
        return "fail"
    if "warn" in statuses:
        return "warn"
    return "pass"


def _normalize_disk_role(disk_type: str | None) -> str | None:
    if disk_type == "system":
        return "system"
    if disk_type == "data":
        return "data"
    return None


def _create_ecs_client(region: str) -> EcsClient:
    return EcsClient(_build_openapi_config(region))


def _create_hbr_client(region: str) -> HbrClient:
    return HbrClient(_build_openapi_config(region))


def _build_openapi_config(region: str) -> open_api_models.Config:
    access_key_id = os.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID")
    access_key_secret = os.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET")
    security_token = os.getenv("ALIBABA_CLOUD_SECURITY_TOKEN")
    if not access_key_id or not access_key_secret:
        raise ValueError(
            "未读取到阿里云访问凭证，请设置环境变量 ALIBABA_CLOUD_ACCESS_KEY_ID 和 "
            "ALIBABA_CLOUD_ACCESS_KEY_SECRET。"
        )
    return open_api_models.Config(
        access_key_id=access_key_id,
        access_key_secret=access_key_secret,
        security_token=security_token,
        region_id=region,
    )


def _build_instance_context(input_data: dict) -> dict:
    return {
        "region": input_data["region"],
        "instance_id": input_data["instance_id"],
        "disk_id": input_data["disk_id"],
        "disk_role": input_data["disk_role"],
        "service_criticality": input_data.get("service_criticality"),
        "zone_id": input_data.get("zone_id"),
        "instance_name": input_data.get("instance_name"),
        "asset_information": input_data.get("asset_information"),
        "topology_information": input_data.get("topology_information"),
    }


def _build_summary(
    input_data: dict,
    instance_info: dict,
    online_expansion_support: dict,
    temporary_disk_risk: dict,
    disk_layout_and_usage: dict,
    backup_management: dict,
    resource_quota: dict,
) -> dict:
    instance_status = instance_info.get("status", "unknown")
    online_status = online_expansion_support.get("status", "unknown")
    temporary_status = temporary_disk_risk.get("status", "unknown")
    layout_status = disk_layout_and_usage.get("status", "unknown")
    backup_status = backup_management.get("status", "unknown")
    quota_status = resource_quota.get("status", "unknown")
    can_online_expand = online_expansion_support.get("can_online_expand", False)
    requires_shutdown = online_expansion_support.get("requires_shutdown", False)
    requires_manual_review = online_expansion_support.get("requires_manual_review", False)
    temporary_business_risk = temporary_disk_risk.get("business_risk", "unknown")

    if instance_status == "fail" or online_status == "fail" or backup_status == "fail":
        overall_readiness = "not_ready"
        risk_level = "high"
    elif temporary_status == "fail":
        overall_readiness = "not_ready"
        risk_level = "high"
    elif online_status == "unknown" and requires_manual_review:
        overall_readiness = "manual_review_required"
        risk_level = "unknown"
    elif instance_status == "warn" or online_status == "warn" or temporary_status == "warn" or layout_status == "warn" or backup_status == "warn" or quota_status == "warn":
        overall_readiness = "conditionally_ready"
        if temporary_business_risk == "high":
            risk_level = "high"
        else:
            risk_level = "medium" if input_data["disk_role"] == "system" else "unknown"
    elif instance_status == "pass" and online_status == "pass":
        overall_readiness = "ready"
        risk_level = "medium" if input_data["disk_role"] == "system" else "low"
    else:
        overall_readiness = "manual_review_required"
        risk_level = "unknown"

    return {
        "overall_readiness": overall_readiness,
        "can_online_expand": can_online_expand,
        "requires_shutdown": requires_shutdown,
        "risk_level": risk_level,
    }


def _build_pending_check(name: str) -> dict:
    return {"status": "unknown", "findings": [f"{name}尚未实现，本地版本暂未形成可靠判断。"]}


def _build_risk_summary(
    input_data: dict,
    instance_info: dict,
    temporary_disk_risk: dict,
    disk_layout_and_usage: dict,
    backup_management: dict,
    resource_quota: dict,
) -> list[str]:
    risks = []
    layout_summary = disk_layout_and_usage.get("layout_summary") or {}
    backup_summary = backup_management.get("backup_summary") or {}

    for missing_item in instance_info.get("missing_context", []):
        if missing_item == "service_criticality":
            risks.append("当前缺少业务重要级别信息，尚无法准确判断本次扩容的业务影响等级。")
        elif missing_item == "asset_information":
            risks.append("当前缺少资产属性信息，尚无法确认环境类型、应用归属或负责人。")
        elif missing_item == "topology_information":
            risks.append("当前缺少拓扑信息，尚无法判断该实例是否为单点实例或集群节点。")

    if input_data["disk_role"] == "system":
        risks.append("目标磁盘为系统盘，后续在线扩容支持性和停机要求需要继续核实。")
    if temporary_disk_risk.get("has_temporary_storage"):
        risks.append("实例内存在临时路径或易失性存储线索，扩容前需确认其中是否存放业务数据。")
    if temporary_disk_risk.get("business_risk") == "high":
        risks.append("临时磁盘中的数据在线路重启或停机过程中可能丢失，业务风险较高。")
    if layout_summary.get("uses_lvm"):
        risks.append("目标磁盘可能涉及 LVM，后续扩容路径和操作步骤会更复杂。")
    if disk_layout_and_usage.get("status") == "warn":
        risks.append("目标磁盘的分区、文件系统或挂载路径仍需人工复核，扩容路径尚未完全确认。")
    if backup_summary.get("has_rollback_basis") is False:
        risks.append("当前未识别到可用于回退的有效备份基础，扩容失败后的恢复保障不足。")
    if backup_management.get("status") == "warn":
        risks.append("当前备份策略、最近一次备份或备份任务状态仍需人工确认。")
    if resource_quota.get("status") == "warn":
        risks.append("当前资源配额检查只能确认账号能力，尚不能直接确认地域级磁盘扩容额度是否充足。")
    return risks


def _build_recommended_next_steps(
    input_data: dict,
    instance_info: dict,
    temporary_disk_risk: dict,
    disk_layout_and_usage: dict,
    backup_management: dict,
    resource_quota: dict,
) -> list[str]:
    steps = []
    layout_summary = disk_layout_and_usage.get("layout_summary") or {}
    backup_summary = backup_management.get("backup_summary") or {}

    if "service_criticality" in instance_info.get("missing_context", []):
        steps.append("补充业务重要级别信息，以便评估扩容变更的影响范围。")
    if "asset_information" in instance_info.get("missing_context", []):
        steps.append("补充资产属性信息，例如环境类型、应用名称和负责人。")
    if "topology_information" in instance_info.get("missing_context", []):
        steps.append("补充拓扑信息，确认该实例是否为单点实例或集群节点。")
    if temporary_disk_risk.get("has_temporary_storage"):
        steps.append("人工复核临时路径中的实际数据用途，确认是否存在业务数据、日志或缓存依赖。")
    if layout_summary.get("uses_lvm"):
        steps.append("确认 LVM 层级关系和后续 PV/VG/LV 扩容步骤。")
    elif disk_layout_and_usage.get("status") == "warn":
        steps.append("复核目标磁盘的挂载点、文件系统和设备路径，确认后续 OS 内扩容路径。")
    if backup_summary.get("has_rollback_basis") is False:
        steps.append("在执行扩容前先补齐有效快照或备份，确保失败后具备回退基础。")
    elif backup_management.get("status") == "warn":
        steps.append("复核最新快照、HBR 策略与最近一次备份任务状态，确认备份保障处于健康状态。")
    if resource_quota.get("status") == "warn":
        steps.append("复核目标地域与账号的磁盘扩容额度，必要时在控制台或配额中心进一步确认。")
    steps.append("继续执行在线扩容支持性检查，判断系统盘扩容是否支持在线处理。")
    return steps


def _build_failed_response(input_data: dict, error_code: str, error_message: str) -> dict:
    return {
        "status": "failed",
        "cloud_provider": input_data.get("cloud_provider"),
        "instance_id": input_data.get("instance_id"),
        "disk_id": input_data.get("disk_id"),
        "target_size_gb": input_data.get("target_size_gb"),
        "summary": {
            "overall_readiness": "not_ready",
            "can_online_expand": False,
            "requires_shutdown": False,
            "risk_level": "unknown",
        },
        "checks": {
            "instance_info": _build_pending_check("实例信息检查"),
            "online_expansion_support": _build_pending_check("在线扩容支持性检查"),
            "temporary_disk_risk": _build_pending_check("临时磁盘与临时数据风险检查"),
            "disk_layout_and_usage": _build_pending_check("磁盘分区与使用情况检查"),
            "backup_management": _build_pending_check("备份管理情况检查"),
            "resource_quota": _build_pending_check("资源配额检查"),
        },
        "risk_summary": [error_message],
        "recommended_next_steps": ["修正输入或查询条件后重新执行预检。"],
        "error": {"code": error_code, "message": error_message},
    }


@mcp.tool()
def disk_expansion_precheck(
    cloud_provider: str,
    region: str,
    instance_id: str,
    disk_id: str,
    target_size_gb: int,
    os_type: str,
    disk_role: str,
    dist_name: str | None = None,
    current_size_gb: int | None = None,
    filesystem_type: str | None = None,
    partition_scheme: str | None = None,
    lvm_in_use: bool | None = None,
    service_criticality: str | None = None,
    change_window: str | None = None,
    provider_constraints: str | None = None,
    asset_information: str | None = None,
    topology_information: str | None = None,
    zone_id: str | None = None,
    instance_name: str | None = None,
    expected_backup_policy: str | None = None,
    quota_scope_hint: str | None = None,
    temporary_disk_hints: str | None = None,
) -> dict:
    input_data = {
        "cloud_provider": cloud_provider,
        "region": region,
        "instance_id": instance_id,
        "disk_id": disk_id,
        "target_size_gb": target_size_gb,
        "os_type": os_type,
        "disk_role": disk_role,
        "dist_name": dist_name,
        "current_size_gb": current_size_gb,
        "filesystem_type": filesystem_type,
        "partition_scheme": partition_scheme,
        "lvm_in_use": lvm_in_use,
        "service_criticality": service_criticality,
        "change_window": change_window,
        "provider_constraints": provider_constraints,
        "asset_information": asset_information,
        "topology_information": topology_information,
        "zone_id": zone_id,
        "instance_name": instance_name,
        "expected_backup_policy": expected_backup_policy,
        "quota_scope_hint": quota_scope_hint,
        "temporary_disk_hints": temporary_disk_hints,
    }

    validation_result = validate_inputs(input_data)
    if not validation_result["valid"]:
        return _build_failed_response(
            input_data,
            validation_result["error"]["code"],
            validation_result["error"]["message"],
        )

    normalized_input = validation_result["normalized_input"]
    instance_info = get_instance_info(normalized_input)
    online_expansion_support = get_online_expansion_support(normalized_input, instance_info)
    temporary_disk_risk = get_temporary_disk_risk(normalized_input, instance_info, online_expansion_support)
    disk_layout_and_usage = get_disk_layout_and_usage(normalized_input, instance_info)
    backup_management = get_backup_management(normalized_input, instance_info)
    resource_quota = get_resource_quota(normalized_input, instance_info)

    summary = _build_summary(
        normalized_input,
        instance_info,
        online_expansion_support,
        temporary_disk_risk,
        disk_layout_and_usage,
        backup_management,
        resource_quota,
    )
    checks = {
        "instance_info": instance_info,
        "online_expansion_support": online_expansion_support,
        "temporary_disk_risk": temporary_disk_risk,
        "disk_layout_and_usage": disk_layout_and_usage,
        "backup_management": backup_management,
        "resource_quota": resource_quota,
    }
    risk_summary = _build_risk_summary(normalized_input, instance_info, temporary_disk_risk, disk_layout_and_usage, backup_management, resource_quota)
    recommended_next_steps = _build_recommended_next_steps(
        normalized_input,
        instance_info,
        temporary_disk_risk,
        disk_layout_and_usage,
        backup_management,
        resource_quota,
    )

    return {
        "status": "success",
        "cloud_provider": normalized_input["cloud_provider"],
        "instance_id": normalized_input["instance_id"],
        "disk_id": normalized_input["disk_id"],
        "target_size_gb": normalized_input["target_size_gb"],
        "summary": summary,
        "checks": checks,
        "risk_summary": risk_summary,
        "recommended_next_steps": recommended_next_steps,
        "error": None,
    }


def main() -> None:
    mcp.run(transport="stdio")
