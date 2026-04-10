# --- START OF FILE src/help_strings.py ---
"""
ZfDash 集中化帮助内容。
用于桌面 GUI 和 Web UI 的工具提示、警告和指导信息。
"""

HELP = {
    # === VDEV 类型 ===
    "vdev_types": {
        "disk": {
            "name": "单盘 (条带)",
            "short": "无冗余。磁盘故障将导致数据丢失。",
            "when_to_use": "仅用于测试。不建议用于重要数据。",
            "min_devices": 1
        },
        "mirror": {
            "name": "镜像",
            "short": "数据复制到所有磁盘。可承受磁盘故障。",
            "when_to_use": "性能与安全的最佳平衡，适合大多数用户。",
            "min_devices": 2,
            "tip": "推荐大多数家庭用户使用。"
        },
        "raidz1": {
            "name": "RAID-Z1",
            "short": "单奇偶校验。可承受1块磁盘故障。",
            "when_to_use": "适合3-4块磁盘。空间与安全的平衡。",
            "min_devices": 3
        },
        "raidz2": {
            "name": "RAID-Z2",
            "short": "双奇偶校验。可承受2块磁盘故障。",
            "when_to_use": "推荐用于5块以上磁盘或大容量磁盘（4TB+）。",
            "min_devices": 4
        },
        "raidz3": {
            "name": "RAID-Z3",
            "short": "三重奇偶校验。可承受3块磁盘故障。",
            "when_to_use": "适用于大容量阵列（8块以上磁盘）和大容量磁盘。",
            "min_devices": 5
        },
        "log": {
            "name": "日志 (SLOG)",
            "short": "独立的意图日志，用于同步写入。",
            "when_to_use": "NFS/iSCSI 服务器，使用 sync=always 的数据库。",
            "warning": "请使用具有断电保护的企业级 SSD。",
            "min_devices": 1
        },
        "cache": {
            "name": "缓存 (L2ARC)",
            "short": "快速存储（SSD）上的读取缓存。",
            "when_to_use": "当工作集大于内存且随机读取频繁时。",
            "tip": "使用内存进行索引。每缓存块约需70字节内存。",
            "min_devices": 1
        },
        "spare": {
            "name": "热备盘",
            "short": "用于自动替换的备用磁盘。",
            "when_to_use": "需要快速恢复的大型阵列。",
            "min_devices": 1
        },
        "special": {
            "name": "特殊 (元数据) - 危险",
            "short": "在快速存储上存储元数据和小文件。",
            "when_to_use": "HDD+SSD 混合存储池以获得更好性能。",
            "warning": "⚠️ 关键：此VDEV丢失将导致存储池完全丢失！请使用'镜像特殊VDEV'以获得冗余！",
            "recommended_alternative": "镜像特殊VDEV",
            "min_devices": 1
        },
        "special mirror": {
            "name": "镜像特殊VDEV (推荐)",
            "short": "快速驱动器上的镜像元数据存储。",
            "when_to_use": "生产环境混合存储池。需要2块以上SSD。",
            "tip": "✓ Fusion Pool / 元数据 VDEV 的安全选择。",
            "min_devices": 2
        },
        "dedup": {
            "name": "去重 (DDT存储) - 危险",
            "short": "去重表的专用存储。",
            "when_to_use": "启用去重时希望DDT在独立快速存储上。",
            "warning": "⚠️ 关键：此VDEV丢失将导致存储池完全丢失！请使用'镜像去重VDEV'！",
            "recommended_alternative": "镜像去重VDEV",
            "min_devices": 1
        },
        "dedup mirror": {
            "name": "镜像去重VDEV (推荐)",
            "short": "镜像去重表存储。",
            "when_to_use": "启用去重的生产环境存储池。",
            "tip": "✓ 启用去重的存储池的安全选择。",
            "min_devices": 2
        }
    },

    # === 空状态消息 ===
    "empty_states": {
        "create_pool_vdev_tree": {
            "title": "尚未配置VDEV",
            "message": "点击'添加VDEV'开始构建存储池布局。",
            "steps": [
                "从下拉菜单选择VDEV类型（如镜像、RAID-Z1）",
                "点击'添加VDEV'创建",
                "从左侧面板选择设备",
                "点击右箭头（→）将设备添加到选中的VDEV",
                "点击VDEV上的垃圾桶图标（🗑）可删除",
                "如需要可重复添加更多VDEV"
            ]
        },
        "add_vdev_modal": {
            "title": "添加VDEV以扩展存储池",
            "message": "选择VDEV类型并添加设备。",
            "steps": [
                "从下拉菜单选择VDEV类型（如镜像、缓存）",
                "点击'添加VDEV'创建",
                "从可用列表选择设备",
                "点击右箭头（→）将设备添加到选中的VDEV",
                "点击VDEV上的垃圾桶图标（🗑）可删除",
                "准备好后点击'确定'将VDEV添加到存储池"
            ]
        },
        "no_pools": {
            "title": "未找到ZFS存储池",
            "message": "创建新存储池或导入现有存储池。",
            "actions": ["创建存储池", "导入存储池"]
        },
        "no_datasets": {
            "title": "此存储池中没有数据集",
            "message": "此存储池尚无子数据集。创建一个来组织您的数据。"
        }
    },

    # === 危险操作 ===
    "warnings": {
        "destroy_pool": {
            "title": "销毁存储池",
            "message": "这将永久删除存储池中的所有数据！",
            "confirm_text": "输入存储池名称以确认："
        },
        "destroy_dataset": {
            "title": "销毁数据集",
            "message": "这将删除数据集及其所有快照。",
            "confirm_text": "输入'destroy'以确认："
        },
        "force_create": {
            "title": "强制选项已启用",
            "message": "使用-f可能绕过安全检查。请谨慎使用。"
        },
        "single_special_vdev": {
            "title": "无冗余！",
            "message": "单个特殊VDEV没有冗余。如果它失败，您将丢失整个存储池！"
        },
        "single_dedup_vdev": {
            "title": "无冗余！",
            "message": "单个去重VDEV没有冗余。如果它失败，您将丢失整个存储池！"
        }
    },

    # === UI元素工具提示 ===
    "tooltips": {
        "pool_name": "存储池名称必须以字母开头。允许：A-Z, a-z, 0-9, _, -, .",
        "force_checkbox": "绕过安全检查（如不同磁盘大小）。请谨慎使用。",
        "show_all_devices": "显示所有块设备，包括分区和潜在不安全的磁盘。",
        "encryption": "为此存储池/数据集启用加密。启用后无法禁用。",
        "compression": "推荐使用LZ4压缩。提供良好的速度与压缩比。",
        "dedup": "去重需要大量内存（每1TB唯一数据约需5GB）。请谨慎使用。"
    },

    # === 通用提示 ===
    "tips": {
        "first_pool": "提示：对于您的第一个存储池，建议使用2块磁盘的'镜像'，既安全又简单。",
        "encryption": "提示：在创建存储池时启用加密以获得完整保护。之后无法添加。",
        "compression": "提示：推荐使用LZ4压缩。对于大多数工作负载既快速又有效。",
        "recordsize": "提示：默认记录大小（128K）适合一般用途。数据库可能受益于更小的值。",
        "fusion_pool": "提示：Fusion Pool = HDD数据VDEV + SSD镜像特殊VDEV。适合混合工作负载。"
    },

    # === 快速参考 ===
    "quick_reference": {
        "vdev_types_summary": {
            "data": ["disk", "mirror", "raidz1", "raidz2", "raidz3"],
            "auxiliary": ["log", "cache", "spare"],
            "special_class": ["special", "special mirror", "dedup", "dedup mirror"]
        },
        "recommended_configs": [
            {"name": "家庭NAS (2块磁盘)", "vdevs": ["mirror"]},
            {"name": "家庭NAS (4块磁盘)", "vdevs": ["mirror", "mirror"]},
            {"name": "大型NAS (6块以上磁盘)", "vdevs": ["raidz2"]},
            {"name": "混合性能", "vdevs": ["raidz1", "special mirror"]}
        ]
    }
}


def get_vdev_help(vdev_type: str) -> dict:
    """获取特定VDEV类型的帮助信息。"""
    return HELP["vdev_types"].get(vdev_type.lower(), {})


def get_empty_state(context: str) -> dict:
    """获取特定UI上下文的空状态消息。"""
    return HELP["empty_states"].get(context, {})


def get_warning(action: str) -> dict:
    """获取危险操作的警告信息。"""
    return HELP["warnings"].get(action, {})


def get_tooltip(element: str) -> str:
    """获取UI元素的工具提示文本。"""
    return HELP["tooltips"].get(element, "")


def get_tip(topic: str) -> str:
    """获取主题的帮助提示。"""
    return HELP["tips"].get(topic, "")


# --- END OF FILE src/help_strings.py ---