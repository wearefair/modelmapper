add_errs1 = [('msg1', 'field1', 'item1'),
             ('msg1', 'field1', 'item2'),
             ('There is a value that is longer than 10.', 'field2', 'item3'), ]


expected_report_str1 = ''.join([
    'field_name    error            count    err%    items\n',
    '------------  ---------------  -------  ------  -------\n',
    'field1        msg1             2        10%     item1\n',
    '                                                item2\n',
    'field2        There is a       1        5%      item3\n',
    '              value that is\n',
    '              longer than 10.'])

expected_report_dict1 = {
    'field_name1': 'field1',
    'error1': 'msg1',
    'count1': 2,
    'err%1': '10%',
    'items1': 'item1, item2',
    'field_name2': 'field2',
    'error2': 'There is a value that is longer than 10.',
    'count2': 1,
    'err%2': '5%',
    'items2': 'item3'
}


add_errs2 = [('msg1', 'field1', 'item1'),
             ('msg1', 'field2', 'item2'),
             ('msg3', 'field2', 'item3'),
             ('msg3', 'field3', 'item4'),
             ]


expected_report_str2 = ''.join(
    ['field_name    error      count  err%    items\n',
     '------------  -------  -------  ------  -------\n',
     'field1        msg1           1  5%      item1\n',
     'field2        msg1           1  5%      item2\n',
     'field2        msg3           1  5%      item3\n',
     'field3        msg3           1  5%      item4']
)

expected_report_dict2 = {
    'field_name1': 'field1',
    'error1': 'msg1',
    'count1': 1,
    'err%1': '5%',
    'items1': 'item1',
    'field_name2': 'field2',
    'error2': 'msg1',
    'count2': 1,
    'err%2': '5%',
    'items2': 'item2',
    'field_name3': 'field3',
    'error3': 'msg3',
    'count3': 1,
    'err%3': '5%',
    'items3': 'item3, item4'
}


add_errs3 = [('msg1', 'field1', 'item1'),
             ('msg1', 'field2', 'item2'),
             ('msg3', 'field3', 'item3'),
             ('msg5', 'field4', 'item4'),
             ]


expected_report_str3 = ''.join(
    ['field_name    error      count  err%    items\n',
     '------------  -------  -------  ------  -------\n',
     'field1        msg1           1  5%      item1\n',
     'field2        msg1           1  5%      item2\n',
     'field3        msg3           1  5%      item3\n',
     'field4        msg5           1  5%      item4']
)

expected_report_dict3 = {
    'field_name1': 'field1',
    'error1': 'msg1',
    'count1': 1,
    'err%1': '5%',
    'items1': 'item1',
    'field_name2': 'field2',
    'error2': 'msg1',
    'count2': 1,
    'err%2': '5%',
    'items2': 'item2',
    'field_name3': 'field3',
    'error3': 'msg3',
    'count3': 1,
    'err%3': '5%',
    'items3': 'item3'
}
