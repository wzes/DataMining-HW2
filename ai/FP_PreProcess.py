import pandas as pd

from fp_growth import find_frequent_itemsets


def tolist(vip, type):
    # sorted the records
    sorteds = vip.sort_values(by=['sldat'])
    # record count
    record_count = len(vip)
    # get 60% datas
    some = sorteds[:int(record_count * 0.6)]

    tmp = some[type].tolist()

    res = []
    for item in tmp:
        if item != 0:
            res.append(item)
    # return
    return res


def tosequential(vip, type):
    # sorted the records
    sorteds = vip.sort_values(by=['sldat'])
    # record count
    record_count = len(vip)
    # get 60% datas
    some = sorteds[:int(record_count * 0.6)]
    # group
    seq = some.groupby('sldat')

    items = seq.count().index.values

    itemsNo = []
    for i in range(len(items)):
        tmp = seq.get_group(items[i])[type].tolist()
        res = []
        for item in tmp:
            if item != 0:
                res.append(item)
        if len(res) != 0:
            itemsNo.append(res)
    # return
    return itemsNo


def toset(some):
    return list(set(some))


def printFre(vips, vipNos, type, support):
    # store the every vip info
    vipPlus = []
    for i in range(len(vips)):
        vipPlus.append(tolist(vips.get_group(vipNos[i]), type))
    frequent_items = find_frequent_itemsets(vipPlus, support)
    #
    return list(frequent_items)


def getBRecords(vips, type):
    records = []
    for i in range(len(vipNos)):
        records.append(tolist(vips.get_group(vipNos[i]), type))
    return records


def getARecords(vips, type):
    records = []
    for i in range(len(vipNos)):
        mlist = tolist(vips.get_group(vipNos[i]), type)
        for item in mlist:
            records.append([item])
    return records


def getASRecords(vips, type):
    records = []
    for i in range(len(vipNos)):
        mlist = tosequential(vips.get_group(vipNos[i]), type)
        for item in mlist:
            for item1 in item:
                records.append([[item1]])
    return records


def getBSRecords(vips, type):
    records = []
    for i in range(len(vipNos)):
        mlist = tosequential(vips.get_group(vipNos[i]), type)
        records.append(mlist)
    return records


if __name__ == "__main__":
    df = pd.read_csv('trade.csv', header=0)
    df['bndno'] = df['bndno'].fillna(0).astype(int)
    # get groups by vipno
    vips = df.groupby('vipno')
    # get all customers
    vipNos = vips.count().index.values

    # # print the set data set
    types = ['pluno', 'dptno', 'bndno']


    # #
    for type in types:
        aRecords = getARecords(vips, type)
        for i in range(len(aRecords)):
            with open('nor_trade_' + type + '_a.txt', 'a+') as f:
                f.write(str(aRecords[i]).replace('[', '').replace(']', '') + '\n')
        print(aRecords)
    for type in types:
        bRecords = getBRecords(vips, type)
        for i in range(len(bRecords)):
            with open('nor_trade_' + type + '_b.txt', 'a+') as f:
                f.write(str(bRecords[i]).replace('[', '').replace(']', '') + '\n')
        print(bRecords)
    for type in types:
        asRecords = getASRecords(vips, type)
        for i in range(len(asRecords)):
            with open('seq_trade_' + type + '_a.txt', 'a+') as f:
                f.write(str(asRecords[i][0][0]).replace('[', '').replace(']', '') + '\n')
        print(asRecords)
    for type in types:
        bsRecords = getBSRecords(vips, type)
        for i in range(len(bsRecords)):
            for j in range(len(bsRecords[i])):
                with open('seq_trade_' + type + '_b.txt', 'a+') as f:
                    f.write(str(bsRecords[i][j]).replace('[', '').replace(']', '') + ';')
            with open('seq_trade_' + type + '_b.txt', 'a+') as f:
                f.write('\n')
        print(bsRecords)

        #
        # # print the sequential data set
        # # print(tosequential(vips.get_group(vipNos[0]), 'pluno'))
        # types = ['pluno', 'dptno', 'bndno']
        # for type in types:
        #     for i in range(len(vipNos)):
        #         with open('seq_trade_' + type + '.txt', 'a+') as f:
        #             f.write(str(tosequential(vips.get_group(vipNos[i]), type)).replace('[', '').replace('],', ';').replace(']', '') + '\n')
        # for i in [2, 4, 8, 16, 32, 64]:
        #     pluFre = printFre(vips, vipNos, 'pluno', i)
        #     dptFre = printFre(vips, vipNos, 'dptno', i)
        #     bndFre = printFre(vips, vipNos, 'bndno', i)
