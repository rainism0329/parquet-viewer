# 开源发布与 JetBrains 授权申请清单

Parquet Viewer 采用 Apache-2.0 开源许可证，第三方组件仍遵循各自许可。本次计划发布 **2.5.2.2**。

这份清单是维护者的操作说明，不代表 JetBrains 已批准项目或保证申请成功。官方页面核对日期：2026-09-29；提交时请再次查看当前要求。

## 1. 发布前确认权利与内容

- [ ] 确认有权将项目原创代码、图标、图片和其他资源按 Apache-2.0 发布，包括可能涉及的雇主或委托方权利。
- [ ] 检查历史外部贡献及复制、改写的代码。需要权利人许可的部分，应取得授权并保留记录；无法确认的内容应先移除或替换。
- [ ] 检查 [THIRD_PARTY_NOTICES.md](../THIRD_PARTY_NOTICES.md) 及发布包中实际包含的依赖，保留其许可证、版权声明和必要的 NOTICE。项目的 Apache-2.0 声明不覆盖第三方自身的许可。
- [ ] 运行测试与插件构建，记录实际结果。检查最终 ZIP 及其中项目 JAR 是否包含 `META-INF/LICENSE`、`META-INF/NOTICE` 和第三方声明；检查依赖 JAR 内的原始许可文件未被移除。
- [ ] 在 IDE 中手动检查打开 Parquet 文件、查看 schema、过滤、翻页与导出等主要功能，确认新版本可正常安装和使用。
- [ ] 发布前使用 [Plugin Verifier](https://plugins.jetbrains.com/docs/intellij/plugin-verifier.html) 核对目标 IDE 的二进制兼容性；`verifyPluginStructure` 只验证包结构，不能替代兼容性验证。

构建和测试命令见 [CONTRIBUTING.md](../CONTRIBUTING.md)。这些检查用于准备可靠的开源发行版，不能替代真实持续开发记录。

## 2. 发布 GitHub 源码与版本

- [ ] 提交并推送本次变更到公开仓库的默认分支；确认未登录时也能访问源码、README、LICENSE、NOTICE 和第三方声明。
- [ ] 为 2.5.2.2 创建对应的 Git tag 和 GitHub Release，附上从该提交构建的插件 ZIP，并在发布说明中列出许可文件、第三方声明和构建文档的完善。
- [ ] 保留历史 tag 和已发布归档，避免覆盖既有发布记录。
- [ ] 核对下表中的实际地址。若更改默认分支名，应同步 README、插件描述和申请中的许可链接。

| 用途 | 地址 |
| --- | --- |
| Repository / Source Code | https://github.com/rainism0329/parquet-viewer |
| License | https://github.com/rainism0329/parquet-viewer/blob/master/LICENSE |
| Issue Tracker | https://github.com/rainism0329/parquet-viewer/issues |
| Marketplace | https://plugins.jetbrains.com/plugin/27306-parquet-viewer |

## 3. 手动同步 JetBrains Marketplace

登录插件维护账号，进入 Parquet Viewer 的管理页面：

- [ ] 上传验证过的 **2.5.2.2** 插件 ZIP。
- [ ] 将 **License** 设置为仓库中 Apache-2.0 的 LICENSE 地址，替换旧个人网站 EULA 链接。
- [ ] 将 **Source Code** 设置为公开仓库地址，并核对 **Issue Tracker** 地址。License 可在 General Information 中编辑，源码和问题跟踪链接可在 Technical Information 中设置。
- [ ] 检查线上插件描述是否使用上传包内的 `plugin.xml`。若此前设置了始终使用网页编辑的描述，需要手动同步新的许可、源码链接及说明。
- [ ] 如保留打赏入口，将链接配置到 **Monetization** 页签的专用 Donation 字段；从 Marketplace 插件描述中移除打赏链接。README 中可以保留自愿支持说明。
- [ ] 等待新版本审核完成后，核对公开页面实际显示的版本、许可和源码地址。

字段位置和描述同步方式来自 [JetBrains Marketplace 发布页面说明](https://plugins.jetbrains.com/docs/marketplace/best-practices-for-listing.html)；打赏要求见同页的 [Donations](https://plugins.jetbrains.com/docs/marketplace/best-practices-for-listing.html#donations) 部分。

## 4. 手动更新个人网站的旧 EULA 说明

- [ ] 检查目前指向的 `https://phil-the-guy.zeabur.app/plugins-eula.html` 页面。
- [ ] 对 **Parquet Viewer** 单独写清采用 Apache-2.0，并链接仓库 LICENSE，移除与开源许可冲突的限制。
- [ ] 如果同一 EULA 页面还用于其他插件，不要直接删除或替换其他插件的条款；只调整 Parquet Viewer 的许可说明。
- [ ] 同步网站中 Parquet Viewer 介绍页的源码与许可链接。

## 5. 准备并提交 JetBrains 开源支持申请

申请入口：[Request for Open Source Development License](https://www.jetbrains.com/shop/eform/opensource)。项目负责人或核心贡献者可以申请；表单要求可见、持续的真实贡献，明确说明非代码提交不计为活跃开发。不要把本次许可证或文档调整作为新增功能活动来描述。

- [ ] 填写项目网站、公开仓库及可直接访问的 LICENSE 地址。
- [ ] 填写真实姓名、本人邮箱、GitHub 账号和实际活跃贡献者人数。
- [ ] 整理真实代码提交、版本发布、问题处理或用户反馈的链接；只填写可核实的下载量、用户数和维护情况。
- [ ] 如实说明项目的商业使用、收入、打赏、赞助或雇主资助等情况；如表单或审核要求补充，按实际情况披露。README 中有自愿打赏入口不等于已经收到收入，也不能据此宣称没有任何资助。
- [ ] 确认所申请的 JetBrains IDE 授权只用于协议允许的非商业开源开发，并仅分配给获授权的活跃贡献者。公司商业项目等其他用途需使用适用的其他授权。
- [ ] 提交前重新核对 [开源支持计划](https://www.jetbrains.com/community/opensource/)、[申请表](https://www.jetbrains.com/shop/eform/opensource) 和 [开源项目订阅协议](https://www.jetbrains.com/legal/docs/toolbox/license_opensource/)。

这里的 IDE 使用限制与项目的 Apache-2.0 许可证是两件事。不要给项目 LICENSE 添加“禁止商用”或“仅限个人使用”等额外限制。

### 可用于申请的英文项目介绍

以下内容描述现有功能。请在带有 Apache-2.0 许可证的源码公开后使用，另行补充你自己的实际贡献、维护记录、资金情况和申请人数；不要把待办事项写成已完成。

> Parquet Viewer is an open-source IntelliJ IDEA plugin for inspecting local Apache Parquet files inside the IDE. It provides a schema view and a paginated data table, column selection, filter expressions, cell inspection, and CSV/JSON export of displayed data. It can also generate Hive DDL and Java POJOs from a Parquet schema. The project is licensed under the Apache License, Version 2.0, with third-party components retaining their respective licenses. The source code and issue tracker are hosted at https://github.com/rainism0329/parquet-viewer, and the plugin is distributed through JetBrains Marketplace.

申请是否获批由 JetBrains 根据提交时的政策和项目实际情况决定；完成本清单不构成资格保证。
