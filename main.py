# coding: utf-8
# author: jwxie - xiejiawei000@gmail.com

import hashlib
import json
import logging
import os
import shutil
import time
from tempfile import NamedTemporaryFile
from urllib.request import urlopen, Request

import pandas
import requests
from aip import AipOcr
from tqdm import tqdm

logger = logging.Logger(__name__)
# BEGIN
_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) ' \
      'Chrome/96.0.4664.55 Safari/537.36 Edg/96.0.1054.34'

APP_ID = '25087602'
API_KEY = 'GmkAjvG9HLYy2F2RUNM0h8pW'
SECRET_KEY = 'HM4efodMjqKgFqqtSuoBaPVyUfigTlYB'
# END
client = AipOcr(APP_ID, API_KEY, SECRET_KEY)

proxies = {
    'http': 'http://localhost:10808',
    'https': 'http://localhost:10808',
}
client.setProxies(proxies)
ctime = time.localtime()


def get_table_link(image_path):
    image = open(image_path, 'rb').read()
    excel_link = None
    response = client.tableRecognitionAsync(image)
    if response:
        try:
            request_id = response['result'][0]['request_id']
        except KeyError as e:
            logger.info(f"获取结果错误，当前的请求响应为{response['error_code']}")
            raise e
        logger.info(f'获取到当前的任务请求id:{request_id}，后续将进行轮询拿结果')

        while True:
            result_response = client.getTableRecognitionResult(request_id)
            if result_response:
                r = result_response['result']
                if r['ret_code'] != 3:
                    time.sleep(3)
                    logger.info(f"{request_id}任务未完成，当前进度为{r['percent']}")
                    continue
                else:
                    excel_link = r['result_data']
                    break
    else:
        logger.info(f'请求错误，错误信息{response.status_code}')
    return excel_link


def download_url_to_file(
        url: str,
        dst: str,
        *,
        hash_prefix: None = None,
        progress: bool = True,
        user_agent: str = 'DNS-FILTER'
) -> None:
    """
    内容下载模块，会先将文件下载到临时文件内，确认下载正常后移动数据到指定目录内

    :param url: 内容的下载链接
    :param dst: 下载后保存的位置
    :param hash_prefix: 下载文件的哈希值
    :param progress: 是否要展示下载进度条(基于tqdm)
    :param user_agent: 下载请求的UA值
    :return: 没有返回，None
    """

    file_size = None  # 获取文件大小
    req = Request(url, headers={"User-Agent": user_agent})
    u = urlopen(req)
    meta = u.info()
    if hasattr(meta, 'getheaders'):
        content_length = meta.getheaders("Content-Length")
    else:
        content_length = meta.get_all("Content-Length")
    if content_length is not None and len(content_length) > 0:
        file_size = int(content_length[0])

    # 确认文件目录是否存在
    dst = os.path.expanduser(dst)
    dst_dir = os.path.dirname(dst)
    f = NamedTemporaryFile(delete=False, dir=dst_dir)

    try:
        if hash_prefix is not None:
            sha256 = hashlib.sha256()
        with tqdm(total=file_size, disable=not progress,
                  unit='B', unit_scale=True, unit_divisor=1024) as pbar:
            while True:
                buffer = u.read(8192)
                if len(buffer) == 0:
                    break
                f.write(buffer)
                if hash_prefix is not None:
                    sha256.update(buffer)
                pbar.update(len(buffer))

        f.close()
        if hash_prefix is not None:  # 做哈希检验
            print('哈希校验中...')
            digest = sha256.hexdigest()
            if digest[:len(hash_prefix)] != hash_prefix:
                raise Exception(rf'校验错误，sha256校验错误 (expected "{hash_prefix}", got "{digest}")')
            print('sha256校验通过')
        shutil.move(f.name, dst)
    finally:
        f.close()
        if os.path.exists(f.name):
            os.remove(f.name)


def read_excel_and_get_target(xls_path):
    df = pandas.read_excel(xls_path)
    columns = df.columns

    pp_index = None
    for col in range(3):
        names = df[columns[0]]
        for row_index, cell_name in names.to_dict().items():
            if not isinstance(cell_name, str):
                continue  # 肯定不是正常列
            if '周莉婷' in cell_name or '周莉' in cell_name or '周婷' in cell_name or '莉婷' in cell_name:
                pp_index = row_index
                break
        if pp_index is None:
            logger.info(f'没有在第{col + 1}列找到泡泡的名字噢 0.0，继续下一列 .. ')
        else:
            break
    pp_data = df.loc[pp_index].to_dict()
    return pp_data


def sent_notice(_data, topic):
    # BEGIN
    push_url = 'http://www.pushplus.plus/send/e52fff7187b1400e870233aee67651d4'
    # END
    r_myself = requests.post(
        url=push_url,
        headers={'Content-Type': 'application/json'},
        data=json.dumps({
            'title': f"{ctime.tm_mon}-{ctime.tm_mday + 1}日排班信息！！",
            'content': _data,
            'topic': topic,
            'template': 'txt',
        }).encode(encoding='utf-8')
    )
    if r_myself:
        print('推送消息成功.')


def get_tomorrow_date_key():
    return time.strftime("%m月%d日", time.localtime(time.time() + 3600 * 24))


if __name__ == '__main__':
    excel_link = get_table_link(image_path='test.png')
    logger.info(f'拿到识别的excel的链接为{excel_link}，后续将进行文件获取')
    target_name = 'test.xls'
    download_url_to_file(url=excel_link, dst='target_name', user_agent=_UA)
    logger.info('表格文件获取成功')
    pp_data = read_excel_and_get_target(target_name)

    notice_info = pp_data[get_tomorrow_date_key()]
    if not isinstance(notice_info, str):
        notice_info = '没有识别到明天的排版信息，注意手动查看噢 0.0'
    sent_notice(_data=notice_info, topic='scheduling')
