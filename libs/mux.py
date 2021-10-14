# -*- coding: utf-8 -*-
import os
import sys
import re
import shutil
import datetime
import time
import logging
from airflow.models import Variable
from airflow.hooks.mysql_hook import MySqlHook
from airflow.exceptions import AirflowFailException
import requests
from requests.auth import HTTPBasicAuth
import json
from glob import glob
import subprocess
from pprint import pprint
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from xxx_config import config
from jobs_db import jobsDB
from misc import *

# python executers for operators
def feedProviders(**kwargs):
    db = MySqlHook(mysql_conn_id='vod_jobs')
    db.run('REPLACE INTO `providers` SET `id`="{}", `tag`="{}", `title`="{}"'.format(
    config['providerId'], config['providerTag'], config['providerName']))

def feedItems(**kwargs):
    jdb = jobsDB()
    ids = getProviderAssetIds(config['providerId'],True,True)
    db = MySqlHook(mysql_conn_id='vod_jobs')
    if len(ids) == 0:
        raise AirflowFailException('Empty id list received from Rimts.')
    for i in ids:
        jdb.setItem(i['id'], i['original_id'], i['parent_id'], config['providerId'], i['type'], i['original_title'].replace("'","").replace('"',''))

def prepareMux(**kwargs):
    # get items for the provider
    jdb = jobsDB()
    ids = jdb.getItems(config['providerId'],['2','3'])
    # (id,original_id,provider_id,title,updated)
    for i in ids:
      # check item state
      status = jdb.getItemStatus(i[0])
      if(status['stage'] >= 1):
          logging.info("Skipping. MUX_PREPARE stage already finished for the item {}".format(i[0]))
          continue
      # start the job
      jdb.updateJobState(i[0],1,'start')
      # check if content directory exists
      d = '{}/{}'.format(config['srcPath'],i[1])
      if os.path.isdir(d):
          # get file contents by file type
          v = glob('{}/{}'.format(d,config['videoFileStruct']))
          aa = glob('{}/{}'.format(d,config['audioFileStruct']))
          ss = glob('{}/{}'.format(d,config['subtitleFileStruct']))
          # filter audio tracks by language
          a = []
          for av in aa:
              am = re.search('.*/[\d]+[-_]{1}(ltu|lit|rus|eng|ori)_.*\.isma$',av)
              if am:
                  a.append(av)
          # filter subtitle tracks by language
          s = []
          for sv in ss:
              am = re.search('.*/[\d]+[-_]{1}(LTU|RUS|ENG)\.stl$',sv)
              if am:
                  s.append(sv)
          # filter and debug media file contents
          if len(v) == 1:
              logging.debug('[V] - {}'.format(v[0]))
          else:
              logging.error('Skipping MUX_PREPARE stage - can not find any video stream for item - {}'.format(i[1]))
              continue
          if len(a) >= 1:
              for pa in a:
                  logging.debug('[A] - {}'.format(pa))
          else:
              logging.error('Skipping MUX_PREPARE stage - can not find any audio stream for item - {}'.format(i[1]))
              continue
          if len(s) > 0:
              for ps in s:
                  logging.debug('[S] - {}'.format(pa))
          # start muxer line construction
          # ffmpeg -i 80094637_8000.ismv -i 80094637_a128_eng.isma -i 80094637_a128_rus.isma -map 0:v -map 1:a -map 2:a -c:v copy -c:a copy -pix_fmt yuv420p -vbsf h264_mp4toannexb -f mpegts -mpegts_flags -metadata 80094637_8000_2.ts
          map = []
          mux = r"/usr/bin/ffmpeg -i '{}'".format(v[0])
          map.append("0:v")
          # add audio tracks
          ai = 1
          for aa in a:
              mux += " -i '{}'".format(aa)
              map.append("{}:a".format(ai))
              ai += 1
          # add mapping
          mmap = " -map ".join(map)
          mux += " -map " + mmap
          # copy sources
          mux += " -c:v copy -c:a copy -pix_fmt yuv420p -vbsf h264_mp4toannexb"
          # out to TS
          mux += " -f mpegts -metadata service_provider='{}' -metadata service_name='{}' '{}/{}/{}.ts'".format(config['serviceProvider'],str(i[3]).replace(' ','_'), config['dstMuxPath'],i[0],i[0])

          # fill in muxing data
          jdb.fillJobStorage(i[0], 1, [mux])

          # create item directory
          idir = '{}/{}'.format(config['dstMuxPath'],i[0])
          if not os.path.exists(idir):
              logging.info('Creating destination directory - {}'.format(idir))
              try:
                  os.mkdir(idir)
              except:
                  raise AirflowFailException('Destination directory {} for muxing can not be craeted'.format(idir))

          # copy subtitle files
          if len(s) > 0:
              for sf in s:
                  logging.info('Copying subtitle file {}'.format(sf))
                  shutil.copy2(sf,idir)

          # finish mux-prepare job
          jdb.updateJobState(i[0],1,'stop')
      else:
          logging.error('Skipping MUX_PREPARE stage as there is not such SOURCE directory - {}'.format(d))
          continue

def doMux(number, **kwargs):
    jdb = jobsDB()
    # sleep for number of seconds to queue items correctlly
    time.sleep(2*int(number))
    logging.info('do Muxing #{}'.format(int(number)))
    # items with finished stage one
    items = jdb.getStageItems(1, True, 3)
    # items = jdb.getStageItems(2, True, 3)
    # j = 1
    for i in items:
        td = '{}/{}'.format(config['dstMuxPath'],i['item_id'])
        tf = '{}/{}.ts'.format(td,i['item_id'])
        if os.path.isfile(tf):
            logging.info("Such file already exists {}. Continue...".format(tf))
            continue

        # skip to the next item, if current one is already on the way, if file already exists
        status = jdb.getItemStatus(i['item_id'])
        if(status['stage'] >= 2 and status['status'] == 'started'):
            logging.info("MUX stage already started for the item {}. Continue...".format(i['item_id']))
            continue

        # logging.info("ITEM {} NO.{} is going to be written.".format(i['item_id'],j))
        # j = j + 1
        # jdb.deleteStageItem(2,i['item_id'])

        # start the job
        jdb.updateJobState(i['item_id'],2,'start',{'file_dst': tf})

        # get file contents by file type
        ts = '{}/{}'.format(config['srcPath'], i['original_id'])
        v = glob('{}/{}'.format(ts,config['videoFileStruct']))
        a = glob('{}/{}'.format(ts,config['audioFileStruct']))
        s = glob('{}/{}'.format(ts,config['subtitleFileStruct']))

        # get command line
        cmd = jdb.extractJobStorage(i['item_id'], 1)
        cmd = split_arg_string(cmd)
        # tmp faulty data fix
        if len(cmd) == 0:
            logging.error("Failed to get multiplex line for item {}".format(str(i['item_id'])))
            continue

        if cmd[0].startswith('["'):
            cmd[0] = '/usr/bin/ffmpeg'
            cmd.pop()
        cmd.insert(1,'-y')

        # create directory
        if not os.path.exists(td):
            try:
                os.mkdir(td)
            except:
                raise AirflowFailException('Directory {} can not be craeted'.format(td))

        # collect metadata
        dvf = []
        for vf in v:
            dvf.append('{}/{}'.format(td,os.path.basename(vf)))
        daf = []
        if len(a) > 0:
            for af in a:
                match = [y for y in config['allowedLangTracks'] if (y in af.lower())]
                if len(match) > 0:
                    daf.append('{}/{}'.format(td,os.path.basename(af)))
        dsf = []
        if len(s) > 0:
            for sf in s:
                match = [y for y in config['allowedLangTracks'] if (y in sf.lower())]
                if len(match) > 0:
                    logging.info('Copying subtitle file {}'.format(sf))
                    shutil.copy2(sf,td)
                    dsf.append('{}/{}'.format(td,os.path.basename(sf)))

        # fill in jobs storage with metadata
        jdb.fillJobStorage(i['item_id'], 2, { 'video':dvf, 'trailer':[], 'audio':daf, 'subtitle':dsf })

        # start transmuxer
        if len(cmd) > 0:
            print(cmd)
            #ret = subprocess.run(cmd,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
            ret = subprocess.run(cmd)
            if ret.returncode == 0 and os.path.isfile(tf):
                # finish mux job
                jdb.updateJobState(i['item_id'], 2, 'stop')
                logging.info('MUX stage of file {} finished successfully!'.format(tf))
            else:
                raise AirflowFailException('Failed to transmux the file {}'.format(tf))
        else:
            logging.error('There is no muxer for item {}'.format(i['item_id']))
