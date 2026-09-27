# 中亚班列货运履约台账

本项目保存中亚班列货运履约台账所需的领域上下文和校验契约，便于服务端功能围绕真实业务参与方展开。当前版本只提供资料读取、结构校验和命令行摘要，数据均为演示用虚构内容。

## 参与方

货代运营经理、承运方、口岸查验人员、客户

## 事实资料

- 南昌开行直达吉尔吉斯斯坦首都比什凯克的中欧（亚）班列
- 班列经霍尔果斯口岸出境，货值超过342万美元
- 南昌国际陆港班列线路增至14条并连接多个铁海联运线路

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 编译

```bash
python3 -m compileall -q src tests
```

## 命令行检查

```bash
python3 -m src.rail_corridor_ledger.context fixtures/context.json
```
