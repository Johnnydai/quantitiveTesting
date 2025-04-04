import requests
from os import environ
from time import sleep
import time
import json
import pandas as pd
import random
import pickle
from itertools import product
from itertools import combinations
from collections import defaultdict
from datetime import datetime
import pickle
 
 
# operation 
basic_ops = ['log', 'reverse', 'inverse', 'rank', 'zscore', 'quantile', 'normalize']
 
ts_ops = ['ts_rank', 'ts_zscore', 'ts_delta', 'ts_sum', 'ts_product', 'ts_std_dev', 
          'ts_mean', 'ts_arg_min', 'ts_arg_max', 'ts_scale', 'ts_quantile']


ts_not_use = ["ts_min", "ts_max", "ts_delay", "ts_median",]
 
arsenal = ["ts_moment", "ts_entropy", "ts_min_max_cps", "ts_min_max_diff", "inst_tvr", 'sigmoid', 
           "ts_decay_exp_window", "ts_percentage", "vector_neut", "vector_proj", "signed_power"]
 
twin_field_ops = ["ts_corr", "ts_covariance", "ts_co_kurtosis", "ts_co_skewness", "ts_theilsen"]
 
group_ops = ['group_neutralize', 'group_rank', 'group_zscore']
 
group_ac_ops = ["group_sum","group_max", "group_mean", "group_median", "group_min", "group_std_dev",]
 
ops_set = basic_ops + ts_ops + arsenal + group_ops

# 账户登录
def login():
    
    username = "1012169960@qq.com"
    password = "Ace144169"
 
    # Create a session to persistently store the headers
    s = requests.Session()
 
    # Save credentials into session
    s.auth = (username, password)
 
    # Send a POST request to the /authentication API
    response = s.post('https://api.worldquantbrain.com/authentication')
    print(response.content)
    print('username:' + username + ' logged in' + 'password:' + password) 
    return s  
 
# 给定 alphaID 获取对应的 alpha 的回测结果 sharpe 等
def locate_alpha(s, alpha_id):
    alpha = s.get("https://api.worldquantbrain.com/alphas/" + alpha_id)
    string = alpha.content.decode('utf-8')
    metrics = json.loads(string)
    #print(metrics["regular"]["code"])
    
    dateCreated = metrics["dateCreated"]
    sharpe = metrics["is"]["sharpe"]
    fitness = metrics["is"]["fitness"]
    turnover = metrics["is"]["turnover"]
    margin = metrics["is"]["margin"]
    
    triple = [sharpe, fitness, turnover, margin, dateCreated]
 
    return triple

# 将 field_list 切割成若干数量为 num 的组
# 返回的是切割之后的二维 list
def list_chuckation(field_list, num):
    list_chucked = []
    lens = len(field_list)
    i = 0
    while i+num <= lens:
        list_chucked.append(field_list[i:i+num])
        i += num
    list_chucked.append(field_list[i:lens])
    return list_chucked

# 对 alpha 进行设置, 如命名, 着色, 分组, 标签等
def set_alpha_properties(
    s,
    alpha_id,
    name: str = None,
    color: str = None,
    selection_desc: str = "None",
    combo_desc: str = "None",
    tags: str = ["ace_tag"],
):
    """
    Function changes alpha's description parameters
    """
 
    params = {
        "color": color,
        "name": name,
        "tags": tags,
        "category": None,
        "regular": {"description": None},
        "combo": {"description": combo_desc},
        "selection": {"description": selection_desc},
    }
    response = s.patch(
        "https://api.worldquantbrain.com/alphas/" + alpha_id, json=params
    )
 
# 对 alpha 进行 check_submission 操作
def check_submission(alpha_bag, gold_bag, start):
    depot = []
    s = login()
    for idx, g in enumerate(alpha_bag):
        if idx < start:
            continue
        if idx % 5 == 0:
            print(idx)
        if idx % 200 == 0:
            s = login()
        #print(idx)
        pc = get_check_submission(s, g)
        if pc == "sleep":
            sleep(100)
            s = login()
            alpha_bag.append(g)
        elif pc != pc:
            # pc is nan
            print("check self-corrlation error")
            sleep(100)
            alpha_bag.append(g)
        elif pc == "fail":
            continue
        elif pc == "error":
            depot.append(g)
        else:
            print(g)
            gold_bag.append((g, pc))
    print(depot)
    return gold_bag

# 获得 check_submission 的结果
def get_check_submission(s, alpha_id):
    while True:
        result = s.get("https://api.worldquantbrain.com/alphas/" + alpha_id + "/check")
        if "retry-after" in result.headers:
            time.sleep(float(result.headers["Retry-After"]))
        else:
            break
    try:
        if result.json().get("is", 0) == 0:
            print("logged out")
            return "sleep"
        checks_df = pd.DataFrame(
                result.json()["is"]["checks"]
        )
        pc = checks_df[checks_df.name == "PROD_CORRELATION"]["value"].values[0]
        if not any(checks_df["result"] == "FAIL"):
            return pc
        else:
            return "fail"
    except:
        print("catch: %s"%(alpha_id))
        return "error"

# 提交一次因子的submit
def submit(s, alpha_id):
    try:
        result =s.post(f"https://api.worldquantbrain.com/alphas/{alpha_id}/submit")
    except:
        print('重新登陆')
        s = login()
        result =s.post(f"https://api.worldquantbrain.com/alphas/{alpha_id}/submit")
    while True:
        if "retry-after" in result.headers:
            # print(f'{alpha_id} submiting {datetime.now()}')
            sleep(60)
            time.sleep(float(result.headers["Retry-After"]))
            try:
                result = s.get(f"https://api.worldquantbrain.com/alphas/{alpha_id}/submit")
            except:
                print('重新登陆')
                s = login()
        else:
            break
    return result

# 给定alpha_list，自动submit因子，并返回其状态
def submit_alpha(alphaid_list, start):
    s = login()
    for i, alphaid in enumerate(alphaid_list):
        if i < start: continue
        count = 1
        while True:
            print(f"{i} {alphaid} 开始第 {count} 次提交")
            res = submit(s, alphaid)
            if res.text:
                try:
                    res = res.json()
                    break
                except:
                    print('当前有submit任务正在进行中，sleeping 2 min')
                    sleep(120)
            else:
                print(f"{i} {alphaid} 第 {count} 次提交超时")
                count += 1
            if count > 5:
                break
        # 超过5次提交超时则先跳过
        if count > 5 and not res.text:
            print(f"{i} {alphaid} 提交次数过多，晚点提交")
            alphaid_list.append(alphaid)
            continue
        
        # 若是输入alphaid错误
        if 'detail' in res:
            if res['detail'] == 'Not found.':
                print(f"{i} {alphaid} 错误")
            elif res['detail'] == 'THROTTLED':
                print(f"{i} {alphaid} 被限流")
                alphaid_list.append(alphaid)
                sleep(300)
            continue
        
        # 检查submit情况
        submitted = True
        for item in res['is']['checks']:
            if item['name'] == 'ALREADY_SUBMITTED':
                submitted = False
                print(f"{i} {alphaid} 已经提交过了")
                break
            if item['result'] == 'FAIL':
                submitted = False
                print(f"{i} {alphaid} 的 {item['name']} 检查不通过")
                print(item)
                break
        if submitted:
            print(f'{i} {alphaid}提交成功')

# 获得 vector 类型的 fields         
def get_vec_fields(fields):

    vec_ops = ['vec_avg', 'vec_sum']
    vec_fields = []
 
    for field in fields:
        for vec_op in vec_ops:
            if vec_op == "vec_choose":
                vec_fields.append("%s(%s, nth=-1)"%(vec_op, field))
                vec_fields.append("%s(%s, nth=0)"%(vec_op, field))
            else:
                vec_fields.append("%s(%s)"%(vec_op, field))
 
    return(vec_fields)

# 进行 simulate
def simulate(alpha_dict, region_dict, name, neut, start, stone_bag):
    
    s = login()
 
    for key, alpha_set in alpha_dict.items():
        print("curr %s len %d"%(key, len(alpha_set)))
        groups = list_chuckation(alpha_set,3)
        for idx, group in enumerate(groups):
            if idx < start: continue
            region, uni = region_dict[key]
            progress_urls = []
            for field, decay in group:
                #alpha = "rank(vec_avg(%s))"%(field)
                #alpha = "%s+%s"%(field, recipe)
                alpha = "%s"%(field)
                print("group %d %s %s %s %s"%(idx, alpha, region, uni, decay))
                simulation_data = {
                        'type': 'REGULAR',
                        'settings': {
                            'instrumentType': 'EQUITY',
                            'region': region, 
                            'universe': uni, 
                            'delay': 1,
                            'decay': decay, 
                            'neutralization': neut,
                            #'neutralization': 'COUNTRY',
                            #'neutralization': 'SECTOR',
                            #'neutralization': 'MARKET',
                            'truncation': 0.08,
                            'pasteurization': 'ON',
                            'unitHandling': 'VERIFY',
                            'nanHandling': 'ON',
                            'language': 'FASTEXPR',
                            'visualization': False,
                        },
                        'regular': alpha}
                try:
                    simulation_response = s.post('https://api.worldquantbrain.com/simulations', json=simulation_data)
                    simulation_progress_url = simulation_response.headers['Location']
                    progress_urls.append(simulation_progress_url)
                except KeyError:
                    print(" loc key error")
                    sleep(600)
                    s = login()
                except:
                    print("1")
                    sleep(600)
                    s = login()
                    
            print("group %d post done"%(idx))
 
            for progress in progress_urls:
                while True:
                    simulation_progress = s.get(progress)
                    if simulation_progress.headers.get("Retry-After", 0) == 0:
                        break
                    #print("Sleeping for " + simulation_progress.headers["Retry-After"] + " seconds")
                    sleep(float(simulation_progress.headers["Retry-After"]))
 
                print("%s done simulating, getting alpha details"%(progress))
                try:
                    alpha_id = simulation_progress.json()["alpha"]
 
                    set_alpha_properties(s, 
                            alpha_id,
                            name = "%s"%name,
                            color = None,)
 
                    stone_bag.append(alpha_id)
 
                except KeyError:
                    print("look into: %s"%progress)
                except:
                    print("other")
 
 
            print("group %d %s simulate done"%(idx, region))
 
    print("stones:" )
    print(len(stone_bag))
    #print("success rate: %.3f"%(float(len(stone_bag2))/len(comb_fields)))
    return stone_bag

# 进行 simulate (saving the  alpha_dict_unSimulated for simlating next time)
def simulate_new(alpha_dict, alpha_dict_unSimulated, region_dict, name, neut, start):
        
    s = login()
    new_alpha_dict = dict(alpha_dict)
    for key, alpha_set in alpha_dict.items():
        print("curr %s len %d"%(key, len(alpha_set)))
        groups = list_chuckation(alpha_set,3)
        for idx, group in enumerate(groups):
            if idx < start: continue
            region, uni = region_dict[key]
            progress_urls = []
            for field, decay in group:
                #alpha = "rank(vec_avg(%s))"%(field)
                #alpha = "%s+%s"%(field, recipe)
                alpha = "%s"%(field)
                print("group %d %s %s %s %s"%(idx, alpha, region, uni, decay))
                simulation_data = {
                        'type': 'REGULAR',
                        'settings': {
                            'instrumentType': 'EQUITY',
                            'region': region, 
                            'universe': uni, 
                            'delay': 1,
                            'decay': decay, 
                            'neutralization': neut,
                            #'neutralization': 'COUNTRY',
                            #'neutralization': 'SECTOR',
                            #'neutralization': 'MARKET',
                            'truncation': 0.08,
                            'pasteurization': 'ON',
                            'unitHandling': 'VERIFY',
                            'nanHandling': 'ON',
                            'language': 'FASTEXPR',
                            'visualization': False,
                        },
                        'regular': alpha}
                try:
                    simulation_response = s.post('https://api.worldquantbrain.com/simulations', json=simulation_data)
                    simulation_progress_url = simulation_response.headers['Location']
                    progress_urls.append(simulation_progress_url)
                except KeyError:
                    print(" loc key error")
                    alpha_dict_unSimulated.update(new_alpha_dict)
                    print("alpha_dict_unSimulated:" )
                    print(len(alpha_dict_unSimulated))
                    sleep(600)
                    s = login()
                except:
                    print("1")
                    alpha_dict_unSimulated.update(new_alpha_dict)
                    print("alpha_dict_unSimulated:" )
                    print(len(alpha_dict_unSimulated))
                    sleep(600)
                    s = login()
                    
            print("group %d post done"%(idx))
 
            for progress in progress_urls:
                while True:
                    simulation_progress = s.get(progress)
                    if simulation_progress.headers.get("Retry-After", 0) == 0:
                        break
                    #print("Sleeping for " + simulation_progress.headers["Retry-After"] + " seconds")
                    sleep(float(simulation_progress.headers["Retry-After"]))
 
                print("%s done simulating, getting alpha details"%(progress))
                try:
                    alpha_id = simulation_progress.json()["alpha"]
 
                    set_alpha_properties(s, 
                            alpha_id,
                            name = "%s"%name,
                            color = None,)
 
                  #  stone_bag.append(alpha_id)
 
                except KeyError:
                    print("look into: %s"%progress)
                    alpha_dict_unSimulated.update(new_alpha_dict)
                    print("alpha_dict_unSimulated:" )
                    print(len(alpha_dict_unSimulated))
                except:
                    print("other")
                    alpha_dict_unSimulated.update(new_alpha_dict)
                    print("alpha_dict_unSimulated:" )
                    print(len(alpha_dict_unSimulated))
 
 
            print("group %d %s simulate done"%(idx, region))
        
        new_alpha_dict.pop(key)
    #print("success rate: %.3f"%(float(len(stone_bag2))/len(comb_fields)))
    return alpha_dict_unSimulated
 
def multi_simulate(alpha_pools, neut, region, universe, start):

    s = login()

    brain_api_url = 'https://api.worldquantbrain.com'

    for x, pool in enumerate(alpha_pools):
        if x < start: continue
        progress_urls = []
        for y, task in enumerate(pool):
            # 10 tasks, 10 alpha in each task
            sim_data_list = generate_sim_data(task, region, universe, neut)
            try:
                simulation_response = s.post('https://api.worldquantbrain.com/simulations', json=sim_data_list)
                simulation_progress_url = simulation_response.headers['Location']
                progress_urls.append(simulation_progress_url)
            except:
                print(" loc key error")
                sleep(600)
                s = login()

        print("pool %d task %d post done"%(x,y))

        for j, progress in enumerate(progress_urls):
            try:
                while True:
                    simulation_progress = s.get(progress)
                    if simulation_progress.headers.get("Retry-After", 0) == 0:
                        break
                    #print("Sleeping for " + simulation_progress.headers["Retry-After"] + " seconds")
                    sleep(float(simulation_progress.headers["Retry-After"]))

                status = simulation_progress.json().get("status", 0)
                if status != "COMPLETE":
                    print("Not complete : %s"%(progress))

                """
                #alpha_id = simulation_progress.json()["alpha"]
                children = simulation_progress.json().get("children", 0)
                children_list = []
                for child in children:
                    child_progress = s.get(brain_api_url + "/simulations/" + child)
                    alpha_id = child_progress.json()["alpha"]

                    set_alpha_properties(s,
                            alpha_id,
                            name = "%s"%name,
                            color = None,)
                """
            except KeyError:
                print("look into: %s"%progress)
            except:
                print("other")


        print("pool %d task %d simulate done"%(x, j))
    
    print("Simulate done")

# 进行回测设置
def generate_sim_data(alpha_list, region, uni, neut):
    sim_data_list = []
    for alpha, decay in alpha_list:
        simulation_data = {
            'type': 'REGULAR',
            'settings': {
                'instrumentType': 'EQUITY',
                'region': region,
                'universe': uni,
                'delay': 1,
                'decay': decay,
                'neutralization': neut,
                'truncation': 0.08,
                'pasteurization': 'ON',
                'unitHandling': 'VERIFY',
                'nanHandling': 'ON',
                'language': 'FASTEXPR',
                'visualization': False,
            },
            'regular': alpha}

        sim_data_list.append(simulation_data)
    return sim_data_list

# 加载
def load_task_pool(alpha_list, limit_of_multi_simulations, limit_of_concurrent_simulations):
    '''
    Input:
        alpha_list : list of (alpha, decay) tuples
        limit_of_multi_simulations : number of simulation in a multi-simulation
        limit_of_multi_simulations : number of simultaneous multi-simulations
    Output:
        task : [10 * (alpha, decay)] for a multi-simulation
        pool : [10 * [10 * (alpha, decay)]] for simultaneous multi-simulations
        pools : [[10 * [10 * (alpha, decay)]]]

    '''
    tasks = [alpha_list[i:i + limit_of_multi_simulations] for i in range(0, len(alpha_list), limit_of_multi_simulations)]
    pools = [tasks[i:i + limit_of_concurrent_simulations] for i in range(0, len(tasks), limit_of_concurrent_simulations)]
    return pools
 
def get_datasets(
    s,
    instrument_type: str = 'EQUITY',
    region: str = 'USA',
    delay: int = 1,
    universe: str = 'TOP3000'
):
    url = "https://api.worldquantbrain.com/data-sets?" +\
        f"instrumentType={instrument_type}&region={region}&delay={str(delay)}&universe={universe}"
    result = s.get(url)
    datasets_df = pd.DataFrame(result.json()['results'])
    return datasets_df

def get_datafields(
    s,
    instrument_type: str = 'EQUITY',
    region: str = 'USA',
    delay: int = 1,
    universe: str = 'TOP3000',
    dataset_id: str = '',
    search: str = ''
):
    if len(search) == 0:
        url_template = "https://api.worldquantbrain.com/data-fields?" +\
            f"&instrumentType={instrument_type}" +\
            f"&region={region}&delay={str(delay)}&universe={universe}&dataset.id={dataset_id}&limit=50" +\
            "&offset={x}"
        count = s.get(url_template.format(x=0)).json()['count'] 
        
    else:
        url_template = "https://api.worldquantbrain.com/data-fields?" +\
            f"&instrumentType={instrument_type}" +\
            f"&region={region}&delay={str(delay)}&universe={universe}&limit=50" +\
            f"&search={search}" +\
            "&offset={x}"
        count = 100
    
    datafields_list = []
    for x in range(0, count, 50):
        datafields = s.get(url_template.format(x=x))
        datafields_list.append(datafields.json()['results'])
 
    datafields_list_flat = [item for sublist in datafields_list for item in sublist]
 
    datafields_df = pd.DataFrame(datafields_list_flat)
    return datafields_df

def process_datafields(df, data_type):

    if data_type == "matrix":
        datafields = df[df['type'] == "MATRIX"]["id"].tolist()
    elif data_type == "vector":
        datafields = get_vec_fields(df[df['type'] == "VECTOR"]["id"].tolist())

    tb_fields = []
    for field in datafields:
        tb_fields.append("winsorize(ts_backfill(%s, 120), std=4)"%field)
    return tb_fields
 
def view_alphas(gold_bag):
    s = login()
    sharp_list = []
    exp_list = []
    for gold, pc in gold_bag:
        
        triple = locate_alpha(s, gold)
        exp_list.append(triple[1])
        info = [triple[2], triple[3], triple[4], triple[5], triple[6]]
        info.append(pc)
        sharp_list.append(info)
 
    sharp_list.sort(reverse=True, key = lambda x : x[3])
    for i in sharp_list:
        print(i)
    return exp_list
 
def locate_alpha(s, alpha_id):
    while True:
        alpha = s.get("https://api.worldquantbrain.com/alphas/" + alpha_id)
        if "retry-after" in alpha.headers:
            time.sleep(float(alpha.headers["Retry-After"]))
        else:
            break
    string = alpha.content.decode('utf-8')
    metrics = json.loads(string)
    #print(metrics["regular"]["code"])
    
    dateCreated = metrics["dateCreated"]
    sharpe = metrics["is"]["sharpe"]
    fitness = metrics["is"]["fitness"]
    turnover = metrics["is"]["turnover"]
    margin = metrics["is"]["margin"]
    decay = metrics["settings"]["decay"]
    exp = metrics['regular']['code']
    
    triple = [alpha_id, exp, sharpe, turnover, fitness, margin, dateCreated, decay]
    return triple
 
 
def get_alphas(start_date, end_date, sharpe_th, fitness_th, region, alpha_num, usage):
    s = login()
    next_alphas = []
    decay_alphas = []
    # 3E large 3C less
    count = 0
    for i in range(0, alpha_num, 100):
        print(i)
        url_e = "https://api.worldquantbrain.com/users/self/alphas?limit=100&offset=%d"%(i) \
                + "&status=UNSUBMITTED%1FIS_FAIL&dateCreated%3E=" + start_date  \
                + "T00:00:00-04:00&dateCreated%3C" + end_date \
                + "T00:00:00-04:00&is.fitness%3E" + str(fitness_th) + "&is.sharpe%3E" \
                + str(sharpe_th) + "&settings.region=" + region + "&order=-is.sharpe&hidden=false&type!=SUPER"
        url_c = "https://api.worldquantbrain.com/users/self/alphas?limit=100&offset=%d"%(i) \
                + "&status=UNSUBMITTED%1FIS_FAIL&dateCreated%3E=" + start_date  \
                + "T00:00:00-04:00&dateCreated%3C" + end_date \
                + "T00:00:00-04:00&is.fitness%3C-" + str(fitness_th) + "&is.sharpe%3C-" \
                + str(sharpe_th) + "&settings.region=" + region + "&order=-is.sharpe&hidden=false&type!=SUPER"
        urls = [url_e]
        if usage != "submit":
            urls.append(url_c)
        for url in urls:
            response = s.get(url)
            #print(response.json())
            try:
                alpha_list = response.json()["results"]
                #print(response.json())
                for j in range(len(alpha_list)):
                    alpha_id = alpha_list[j]["id"]
                    name = alpha_list[j]["name"]
                    dateCreated = alpha_list[j]["dateCreated"]
                    sharpe = alpha_list[j]["is"]["sharpe"]
                    fitness = alpha_list[j]["is"]["fitness"]
                    turnover = alpha_list[j]["is"]["turnover"]
                    margin = alpha_list[j]["is"]["margin"]
                    longCount = alpha_list[j]["is"]["longCount"]
                    shortCount = alpha_list[j]["is"]["shortCount"]
                    decay = alpha_list[j]["settings"]["decay"]
                    exp = alpha_list[j]['regular']['code']
                    count += 1
                    #if (sharpe > 1.2 and sharpe < 1.6) or (sharpe < -1.2 and sharpe > -1.6):
                    if (longCount + shortCount) > 100:
                        if sharpe < -1.2:
                            exp = "-%s"%exp
                        rec = [alpha_id, exp, sharpe, turnover, fitness, margin, dateCreated, decay]
                        print(rec)
                        if turnover > 0.7:
                            rec.append(decay*4)
                            decay_alphas.append(rec)
                        elif turnover > 0.6:
                            rec.append(decay*3+3)
                            decay_alphas.append(rec)
                        elif turnover > 0.5:
                            rec.append(decay*3)
                            decay_alphas.append(rec)
                        elif turnover > 0.4:
                            rec.append(decay*2)
                            decay_alphas.append(rec)
                        elif turnover > 0.35:
                            rec.append(decay+4)
                            decay_alphas.append(rec)
                        elif turnover > 0.3:
                            rec.append(decay+2)
                            decay_alphas.append(rec)
                        else:
                            next_alphas.append(rec)
            except:
                print("%d finished re-login"%i)
                s = login()

    output_dict = {"next" : next_alphas, "decay" : decay_alphas}
    print("count: %d"%count)
    return output_dict

def prune(next_alpha_recs, region, prefix, keep_num):
    # prefix is datafield prefix, like fnd6, mdl175 ...
    # keep_num is the num of top sharpe same-field alpha to keep 
    output = []
    num_dict = defaultdict(int)
    for rec in next_alpha_recs:
        exp = rec[1]
        field = exp.split(prefix)[-1].split(",")[0]
        sharpe = rec[2]
        if sharpe < 0:
            field = "-%s"%field
        if num_dict[field] < keep_num:
            num_dict[field] += 1
            decay = rec[-1]
            exp = rec[1]
            output.append([exp,decay])
    output_dict = {region : output}
    return output_dict
 
def transform(next_alpha_recs, region):
    output = []
    for rec in next_alpha_recs:
        
        decay = rec[-1]
        exp = rec[1]
        output.append([exp,decay])
    output_dict = {region : output}
    return output_dict
 
def first_order_factory(fields, ops_set):

    alpha_set = []
    for field in fields:
        #reverse op does the work
        alpha_set.append(field)
        #alpha_set.append("-%s"%field)
        for op in ops_set:
 
            if op == "ts_percentage":
 
                #lpha_set += ts_comp_factory(op, field, "percentage", [0.2, 0.5, 0.8])
                alpha_set += ts_comp_factory(op, field, "percentage", [0.5])
 
 
            elif op == "ts_decay_exp_window":
 
                #alpha_set += ts_comp_factory(op, field, "factor", [0.2, 0.5, 0.8])
                alpha_set += ts_comp_factory(op, field, "factor", [0.5])
 
            elif op == "ts_moment":
 
                alpha_set += ts_comp_factory(op, field, "k", [2, 3, 4])
 
            elif op == "ts_entropy":
 
                #alpha_set += ts_comp_factory(op, field, "buckets", [5, 10, 15, 20])
                alpha_set += ts_comp_factory(op, field, "buckets", [10])
 
            elif op.startswith("ts_") or op == "inst_tvr":
 
                alpha_set += ts_factory(op, field)
 
            elif op.startswith("group_"):
 
                alpha_set += group_factory(op, field, "usa")
 
            elif op.startswith("vector"):
 
                alpha_set += vector_factory(op, field)
 
            elif op == "signed_power":
 
                alpha = "%s(%s, 2)"%(op, field)
                alpha_set.append(alpha)
 
            else:
                alpha = "%s(%s)"%(op, field)
                alpha_set.append(alpha)
 
    return alpha_set
    
def get_group_second_order_factory(first_order, group_ops, region):
    second_order = []
    for fo in first_order:
        for group_op in group_ops:
            second_order += group_factory(group_op, fo, region)
    return second_order
 
def get_ts_second_order_factory(first_order, ts_ops):
    second_order = []
    for fo in first_order:
        for ts_op in ts_ops:
            second_order += ts_factory(ts_op, fo)
    return second_order
 
 
def get_data_fields_csv(filename, prefix):
    '''
    inputs: 
    CSV file with header 'field' 
    outputs:
    A list of string
    '''
    df = pd.read_csv(filename,header=0,encoding = 'unicode_escape')
    collection = []
    for _, row in df.iterrows():
        if row['field'].startswith(prefix):
            collection.append(row['field'])
 
    return collection
 
def ts_arith_factory(ts_op, arith_op, field):
    first_order = "%s(%s)"%(arith_op, field)
    second_order = ts_factory(ts_op, first_order)
    return second_order
 
def arith_ts_factory(arith_op, ts_op, field):
    second_order = []
    first_order = ts_factory(ts_op, field)
    for fo in first_order:
        second_order.append("%s(%s)"%(arith_op, fo))
    return second_order
 
def ts_group_factory(ts_op, group_op, field, region):
    second_order = []
    first_order = group_factory(group_op, field, region)
    for fo in first_order:
        second_order += ts_factory(ts_op, fo)
    return second_order
 
def group_ts_factory(group_op, ts_op, field, region):
    second_order = []
    first_order = ts_factory(ts_op, field)
    for fo in first_order:
        second_order += group_factory(group_op, fo, region)
    return second_order
 
def vector_factory(op, field):
    output = []
    vectors = ["cap"]
    
    for vector in vectors:
    
        alpha = "%s(%s, %s)"%(op, field, vector)
        output.append(alpha)
    
    return output
 
def trade_when_factory(op,field,region):
    output = []
    open_events = ["ts_arg_max(volume, 5) == 0", "ts_corr(close, volume, 20) < 0",
                   "ts_corr(close, volume, 5) < 0", "ts_mean(volume,10)>ts_mean(volume,60)",
                   "group_rank(ts_std_dev(returns,60), sector) > 0.7", "ts_zscore(returns,60) > 2",
                   "ts_skewness(returns,120)> 0.7", "ts_arg_min(volume, 5) > 3",
                   "ts_std_dev(returns, 5) > ts_std_dev(returns, 20)",
                   "ts_arg_max(close, 5) == 0", "ts_arg_max(close, 20) == 0",
                   "ts_corr(close, volume, 5) > 0", "ts_corr(close, volume, 5) > 0.3", "ts_corr(close, volume, 5) > 0.5",
                   "ts_corr(close, volume, 20) > 0", "ts_corr(close, volume, 20) > 0.3", "ts_corr(close, volume, 20) > 0.5",
                   "ts_regression(returns, %s, 5, lag = 0, rettype = 2) > 0"%field,
                   "ts_regression(returns, %s, 20, lag = 0, rettype = 2) > 0"%field,
                   "ts_regression(returns, ts_step(20), 20, lag = 0, rettype = 2) > 0",
                   "ts_regression(returns, ts_step(5), 5, lag = 0, rettype = 2) > 0"]

    exit_events = ["abs(returns) > 0.1", "-1", "days_from_last_change(ern3_pre_reptime) > 20"]

    usa_events = ["rank(rp_css_business) > 0.8", "ts_rank(rp_css_business, 22) > 0.8", "rank(vec_avg(mws82_sentiment)) > 0.8",
                  "ts_rank(vec_avg(mws82_sentiment),22) > 0.8", "rank(vec_avg(nws48_ssc)) > 0.8",
                  "ts_rank(vec_avg(nws48_ssc),22) > 0.8", "rank(vec_avg(mws50_ssc)) > 0.8", "ts_rank(vec_avg(mws50_ssc),22) > 0.8",
                  "ts_rank(vec_sum(scl12_alltype_buzzvec),22) > 0.9", "pcr_oi_270 < 1", "pcr_oi_270 > 1",]

    asi_events = ["rank(vec_avg(mws38_score)) > 0.8", "ts_rank(vec_avg(mws38_score),22) > 0.8"]

    eur_events = ["rank(rp_css_business) > 0.8", "ts_rank(rp_css_business, 22) > 0.8",
                  "rank(vec_avg(oth429_research_reports_fundamental_keywords_4_method_2_pos)) > 0.8",
                  "ts_rank(vec_avg(oth429_research_reports_fundamental_keywords_4_method_2_pos),22) > 0.8",
                  "rank(vec_avg(mws84_sentiment)) > 0.8", "ts_rank(vec_avg(mws84_sentiment),22) > 0.8",
                  "rank(vec_avg(mws85_sentiment)) > 0.8", "ts_rank(vec_avg(mws85_sentiment),22) > 0.8",
                  "rank(mdl110_analyst_sentiment) > 0.8", "ts_rank(mdl110_analyst_sentiment, 22) > 0.8",
                  "rank(vec_avg(nws3_scores_posnormscr)) > 0.8",
                  "ts_rank(vec_avg(nws3_scores_posnormscr),22) > 0.8",
                  "rank(vec_avg(mws36_sentiment_words_positive)) > 0.8",
                  "ts_rank(vec_avg(mws36_sentiment_words_positive),22) > 0.8"]

    glb_events = ["rank(vec_avg(mdl109_news_sent_1m)) > 0.8",
                  "ts_rank(vec_avg(mdl109_news_sent_1m),22) > 0.8",
                  "rank(vec_avg(nws20_ssc)) > 0.8",
                  "ts_rank(vec_avg(nws20_ssc),22) > 0.8",
                  "vec_avg(nws20_ssc) > 0",
                  "rank(vec_avg(nws20_bee)) > 0.8",
                  "ts_rank(vec_avg(nws20_bee),22) > 0.8",
                  "rank(vec_avg(nws20_qmb)) > 0.8",
                  "ts_rank(vec_avg(nws20_qmb),22) > 0.8"]

    chn_events = ["rank(vec_avg(oth111_xueqiunaturaldaybasicdivisionstat_senti_conform)) > 0.8",
                  "ts_rank(vec_avg(oth111_xueqiunaturaldaybasicdivisionstat_senti_conform),22) > 0.8",
                  "rank(vec_avg(oth111_gubanaturaldaydevicedivisionstat_senti_conform)) > 0.8",
                  "ts_rank(vec_avg(oth111_gubanaturaldaydevicedivisionstat_senti_conform),22) > 0.8",
                  "rank(vec_avg(oth111_baragedivisionstat_regi_senti_conform)) > 0.8",
                  "ts_rank(vec_avg(oth111_baragedivisionstat_regi_senti_conform),22) > 0.8"]

    kor_events = ["rank(vec_avg(mdl110_analyst_sentiment)) > 0.8",
                  "ts_rank(vec_avg(mdl110_analyst_sentiment),22) > 0.8",
                  "rank(vec_avg(mws38_score)) > 0.8",
                  "ts_rank(vec_avg(mws38_score),22) > 0.8"]

    twn_events = ["rank(vec_avg(mdl109_news_sent_1m)) > 0.8",
                  "ts_rank(vec_avg(mdl109_news_sent_1m),22) > 0.8",
                  "rank(rp_ess_business) > 0.8",
                  "ts_rank(rp_ess_business,22) > 0.8"]

    for oe in open_events:
        for ee in exit_events:
            alpha = "%s(%s, %s, %s)"%(op, oe, field, ee)
            output.append(alpha)
    return output
 
def ts_factory(op, field):
    output = []
    #days = [3, 5, 10, 20, 60, 120, 240]
    days = [5, 22, 66, 120, 240]
    
    for day in days:
    
        alpha = "%s(%s, %d)"%(op, field, day)
        output.append(alpha)
    
    return output
 
def ts_comp_factory(op, field, factor, paras):
    output = []
    #l1, l2 = [3, 5, 10, 20, 60, 120, 240], paras
    l1, l2 = [5, 22, 66, 240], paras
    comb = list(product(l1, l2))
    
    for day,para in comb:
        
        if type(para) == float:
            alpha = "%s(%s, %d, %s=%.1f)"%(op, field, day, factor, para)
        elif type(para) == int:
            alpha = "%s(%s, %d, %s=%d)"%(op, field, day, factor, para)
            
        output.append(alpha)
    
    return output
 
def twin_field_factory(op, field, fields):
    
    output = []
    #days = [3, 5, 10, 20, 60, 120, 240]
    days = [5, 22, 66, 240]
    outset = list(set(fields) - set([field]))
    
    for day in days:
        for counterpart in outset:
            alpha = "%s(%s, %s, %d)"%(op, field, counterpart, day)
            output.append(alpha)
    
    return output
 
 
def group_factory(op, field, region):
    output = []
    vectors = ["cap"] 
    
    chn_group_13 = ['pv13_h_min2_sector', 'pv13_di_6l', 'pv13_rcsed_6l', 'pv13_di_5l', 'pv13_di_4l', 
                        'pv13_di_3l', 'pv13_di_2l', 'pv13_di_1l', 'pv13_parent', 'pv13_level']
    
    
    chn_group_1 = ['sta1_top3000c30','sta1_top3000c20','sta1_top3000c10','sta1_top3000c2','sta1_top3000c5']
    
    chn_group_2 = ['sta2_top3000_fact4_c10','sta2_top2000_fact4_c50','sta2_top3000_fact3_c20']

    # 这边是下线了, 无法使用
    # chn_group_7 = ['oth171_region_sector_long_d1_sector', 'oth171_region_sector_short_d1_sector', 
    #                'oth171_sector_long_d1_sector', 'oth171_sector_short_d1_sector']
    
    hkg_group_13 = ['pv13_10_f3_g2_minvol_1m_sector', 'pv13_10_minvol_1m_sector', 'pv13_20_minvol_1m_sector', 
                    'pv13_2_minvol_1m_sector', 'pv13_5_minvol_1m_sector', 'pv13_1l_scibr', 'pv13_3l_scibr',
                    'pv13_2l_scibr', 'pv13_4l_scibr', 'pv13_5l_scibr']
    
    hkg_group_1 = ['sta1_allc50','sta1_allc5','sta1_allxjp_513_c20','sta1_top2000xjp_513_c5']
    
    hkg_group_2 = ['sta2_all_xjp_513_all_fact4_c10','sta2_top2000_xjp_513_top2000_fact3_c10',
                   'sta2_allfactor_xjp_513_13','sta2_top2000_xjp_513_top2000_fact3_c20']
    
    hkg_group_8 = ['oth455_relation_n2v_p10_q50_w5_kmeans_cluster_5',
                     'oth455_relation_n2v_p10_q50_w4_kmeans_cluster_10',
                     'oth455_relation_n2v_p10_q50_w1_kmeans_cluster_20',
                     'oth455_partner_n2v_p50_q200_w4_kmeans_cluster_5', 
                     'oth455_partner_n2v_p10_q50_w4_pca_fact3_cluster_10',
                     'oth455_customer_n2v_p50_q50_w1_kmeans_cluster_5']
    
    twn_group_13 = ['pv13_2_minvol_1m_sector','pv13_20_minvol_1m_sector','pv13_10_minvol_1m_sector',
                    'pv13_5_minvol_1m_sector','pv13_10_f3_g2_minvol_1m_sector','pv13_5_f3_g2_minvol_1m_sector',
                    'pv13_2_f4_g3_minvol_1m_sector']
    
    twn_group_1 = ['sta1_allc50','sta1_allxjp_513_c50','sta1_allxjp_513_c20','sta1_allxjp_513_c2',
                   'sta1_allc20','sta1_allxjp_513_c5','sta1_allxjp_513_c10','sta1_allc2','sta1_allc5']
    
    twn_group_2 = ['sta2_allfactor_xjp_513_0','sta2_all_xjp_513_all_fact3_c20',
                   'sta2_all_xjp_513_all_fact4_c20','sta2_all_xjp_513_all_fact4_c50']
    
    twn_group_8 = ['oth455_relation_n2v_p50_q200_w1_pca_fact1_cluster_20',
                     'oth455_relation_n2v_p10_q50_w3_kmeans_cluster_20',
                     'oth455_relation_roam_w3_pca_fact2_cluster_5',
                     'oth455_relation_n2v_p50_q50_w2_pca_fact2_cluster_10', 
                     'oth455_relation_n2v_p10_q200_w5_pca_fact2_cluster_20',
                     'oth455_relation_n2v_p50_q50_w5_kmeans_cluster_5']
    
    usa_group_13 = ['pv13_h_min2_3000_sector','pv13_r2_min20_3000_sector','pv13_r2_min2_3000_sector',
                    'pv13_r2_min2_3000_sector', 'pv13_h_min2_focused_pureplay_3000_sector']
    
    usa_group_1 = ['sta1_top3000c50','sta1_allc20','sta1_allc10','sta1_top3000c20','sta1_allc5']
    
    usa_group_2 = ['sta2_top3000_fact3_c50','sta2_top3000_fact4_c20','sta2_top3000_fact4_c10']
    
    usa_group_3 = ['sta3_2_sector', 'sta3_3_sector', 'sta3_news_sector', 'sta3_peer_sector',
                   'sta3_pvgroup1_sector', 'sta3_pvgroup2_sector', 'sta3_pvgroup3_sector', 'sta3_sec_sector']
    
    usa_group_4 = ['rsk69_01c_1m', 'rsk69_57c_1m', 'rsk69_02c_2m', 'rsk69_5c_2m', 'rsk69_02c_1m',
                   'rsk69_05c_2m', 'rsk69_57c_2m', 'rsk69_5c_1m', 'rsk69_05c_1m', 'rsk69_01c_2m']
    
    usa_group_5 = ['anl52_2000_backfill_d1_05c', 'anl52_3000_d1_05c', 'anl52_3000_backfill_d1_02c', 
                   'anl52_3000_backfill_d1_5c', 'anl52_3000_backfill_d1_05c', 'anl52_3000_d1_5c']
    
    usa_group_6 = ['mdl10_group_name']
    
    # usa_group_7 = ['oth171_region_sector_long_d1_sector', 'oth171_region_sector_short_d1_sector', 
    #                'oth171_sector_long_d1_sector', 'oth171_sector_short_d1_sector']
    usa_group_7 = []
    
    usa_group_8 = ['oth455_competitor_n2v_p10_q50_w1_kmeans_cluster_10',
                     'oth455_customer_n2v_p10_q50_w5_kmeans_cluster_10',
                     'oth455_relation_n2v_p50_q200_w5_kmeans_cluster_20',
                     'oth455_competitor_n2v_p50_q50_w3_kmeans_cluster_10', 
                     'oth455_relation_n2v_p50_q50_w3_pca_fact2_cluster_10', 
                     'oth455_partner_n2v_p10_q50_w2_pca_fact2_cluster_5',
                     'oth455_customer_n2v_p50_q50_w3_kmeans_cluster_5',
                     'oth455_competitor_n2v_p50_q200_w5_kmeans_cluster_20']
    
    
    asi_group_13 = ['pv13_20_minvol_1m_sector', 'pv13_5_f3_g2_minvol_1m_sector', 'pv13_10_f3_g2_minvol_1m_sector',
                    'pv13_2_f4_g3_minvol_1m_sector', 'pv13_10_minvol_1m_sector', 'pv13_5_minvol_1m_sector']
    
    asi_group_1 = ['sta1_allc50', 'sta1_allc10', 'sta1_minvol1mc50','sta1_minvol1mc20',
                   'sta1_minvol1m_normc20', 'sta1_minvol1m_normc50']
    
    asi_group_8 = ['oth455_partner_roam_w3_pca_fact1_cluster_5',
                   'oth455_relation_roam_w3_pca_fact1_cluster_20',
                   'oth455_relation_roam_w3_kmeans_cluster_20',
                   'oth455_relation_n2v_p10_q200_w5_pca_fact1_cluster_20',
                   'oth455_relation_n2v_p10_q200_w5_pca_fact1_cluster_20',
                   'oth455_competitor_n2v_p10_q200_w1_kmeans_cluster_10']
    
    jpn_group_1 = ['sta1_alljpn_513_c5', 'sta1_alljpn_513_c50', 'sta1_alljpn_513_c2', 'sta1_alljpn_513_c20']
    
    jpn_group_2 = ['sta2_top2000_jpn_513_top2000_fact3_c20', 'sta2_all_jpn_513_all_fact1_c5',
                   'sta2_allfactor_jpn_513_9', 'sta2_all_jpn_513_all_fact1_c10']
    
    jpn_group_8 = ['oth455_customer_n2v_p50_q50_w5_kmeans_cluster_10', 
                   'oth455_customer_n2v_p50_q50_w4_kmeans_cluster_10', 
                   'oth455_customer_n2v_p50_q50_w3_kmeans_cluster_10', 
                   'oth455_customer_n2v_p50_q50_w2_kmeans_cluster_10',
                   'oth455_customer_n2v_p50_q200_w5_kmeans_cluster_10',
                   'oth455_customer_n2v_p50_q200_w5_kmeans_cluster_10']
    
    jpn_group_13 = ['pv13_2_minvol_1m_sector', 'pv13_2_f4_g3_minvol_1m_sector', 'pv13_10_minvol_1m_sector',
                    'pv13_10_f3_g2_minvol_1m_sector', 'pv13_all_delay_1_parent', 'pv13_all_delay_1_level']
    
    kor_group_13 = ['pv13_10_f3_g2_minvol_1m_sector', 'pv13_5_minvol_1m_sector', 'pv13_5_f3_g2_minvol_1m_sector',
                    'pv13_2_minvol_1m_sector', 'pv13_20_minvol_1m_sector', 'pv13_2_f4_g3_minvol_1m_sector']
    
    kor_group_1 = ['sta1_allc20','sta1_allc50','sta1_allc2','sta1_allc10','sta1_minvol1mc50',
                   'sta1_allxjp_513_c10', 'sta1_top2000xjp_513_c50']
    
    kor_group_2 =['sta2_all_xjp_513_all_fact1_c50','sta2_top2000_xjp_513_top2000_fact2_c50',
                  'sta2_all_xjp_513_all_fact4_c50','sta2_all_xjp_513_all_fact4_c5']
    
    kor_group_8 = ['oth455_relation_n2v_p50_q200_w3_pca_fact3_cluster_5',
                     'oth455_relation_n2v_p50_q50_w4_pca_fact2_cluster_10',
                     'oth455_relation_n2v_p50_q200_w5_pca_fact2_cluster_5',
                     'oth455_relation_n2v_p50_q200_w4_kmeans_cluster_10', 
                     'oth455_relation_n2v_p10_q50_w1_kmeans_cluster_10', 
                     'oth455_relation_n2v_p50_q50_w5_pca_fact1_cluster_20']
    
    eur_group_13 = ['pv13_5_sector', 'pv13_2_sector', 'pv13_v3_3l_scibr', 'pv13_v3_2l_scibr', 'pv13_2l_scibr',
                    'pv13_52_sector', 'pv13_v3_6l_scibr', 'pv13_v3_4l_scibr', 'pv13_v3_1l_scibr']
    
    eur_group_1 = ['sta1_allc10', 'sta1_allc2', 'sta1_top1200c2', 'sta1_allc20', 'sta1_top1200c10']
    
    eur_group_2 = ['sta2_top1200_fact3_c50','sta2_top1200_fact3_c20','sta2_top1200_fact4_c50']
    
    eur_group_3 = ['sta3_6_sector', 'sta3_pvgroup4_sector', 'sta3_pvgroup5_sector']
    
    # eur_group_7 = ['oth171_region_sector_long_d1_sector', 'oth171_region_sector_short_d1_sector', 
    #                'oth171_sector_long_d1_sector', 'oth171_sector_short_d1_sector']
    eur_group_7 = []
    
    eur_group_8 = ['oth455_relation_n2v_p50_q200_w3_pca_fact1_cluster_5',
                     'oth455_competitor_n2v_p50_q200_w4_kmeans_cluster_20',
                     'oth455_competitor_n2v_p50_q200_w5_pca_fact1_cluster_10', 
                     'oth455_competitor_roam_w4_pca_fact2_cluster_20', 
                     'oth455_relation_n2v_p10_q200_w2_pca_fact2_cluster_20', 
                     'oth455_competitor_roam_w2_pca_fact3_cluster_20']
    
    glb_group_13 = ["pv13_10_f2_g3_sector", "pv13_2_f3_g2_sector", "pv13_2_sector", "pv13_52_all_delay_1_sector"]
    
    glb_group_3 = ['sta3_2_sector', 'sta3_3_sector', 'sta3_news_sector', 'sta3_peer_sector',
                   'sta3_pvgroup1_sector', 'sta3_pvgroup2_sector', 'sta3_pvgroup3_sector', 'sta3_sec_sector']
    
    glb_group_1 = ['sta1_allc20', 'sta1_allc10', 'sta1_allc50', 'sta1_allc5']
    
    glb_group_2 = ['sta2_all_fact4_c50', 'sta2_all_fact4_c20', 'sta2_all_fact3_c20', 'sta2_all_fact4_c10']
    
    glb_group_13 = ['pv13_2_sector', 'pv13_10_sector', 'pv13_3l_scibr', 'pv13_2l_scibr', 'pv13_1l_scibr',
                    'pv13_52_minvol_1m_all_delay_1_sector','pv13_52_minvol_1m_sector','pv13_52_minvol_1m_sector']
    
    # glb_group_7 = ['oth171_region_sector_long_d1_sector', 'oth171_region_sector_short_d1_sector', 
    #                'oth171_sector_long_d1_sector', 'oth171_sector_short_d1_sector']  
    glb_group_7 = []
    
    glb_group_8 = ['oth455_relation_n2v_p10_q200_w5_kmeans_cluster_5',
                     'oth455_relation_n2v_p10_q50_w2_kmeans_cluster_5',
                     'oth455_relation_n2v_p50_q200_w5_kmeans_cluster_5', 
                     'oth455_customer_n2v_p10_q50_w4_pca_fact3_cluster_20', 
                     'oth455_competitor_roam_w2_pca_fact1_cluster_10', 
                     'oth455_relation_n2v_p10_q200_w2_kmeans_cluster_5']
    
    amr_group_13 = ['pv13_4l_scibr', 'pv13_1l_scibr', 'pv13_hierarchy_min51_f1_sector',
                    'pv13_hierarchy_min2_600_sector', 'pv13_r2_min2_sector', 'pv13_h_min20_600_sector']
    
    amr_group_3 = ['sta3_news_sector', 'sta3_peer_sector', 'sta3_pvgroup1_sector', 'sta3_pvgroup2_sector',
                   'sta3_pvgroup3_sector']
    
    amr_group_8 = ['oth455_relation_roam_w1_pca_fact2_cluster_10', 
                   'oth455_competitor_n2v_p50_q50_w4_kmeans_cluster_10', 
                   'oth455_competitor_n2v_p50_q50_w3_kmeans_cluster_10', 
                   'oth455_competitor_n2v_p50_q50_w2_kmeans_cluster_10', 
                   'oth455_competitor_n2v_p50_q50_w1_kmeans_cluster_10',
                   'oth455_competitor_n2v_p50_q200_w5_kmeans_cluster_10']
    
    # 下线了, 无法使用
    # group_3 = ["oth171_region_sector_long_d1_sector", "oth171_region_sector_short_d1_sector",
    #            "oth171_sector_long_d1_sector", "oth171_sector_short_d1_sector"]
    group_3 = []
    
    bps_group = "bucket(rank(fnd28_value_05480/close), range='0.2, 1, 0.2')"
    cap_group = "bucket(rank(cap), range='0.1, 1, 0.1')"
    sector_cap_group = "bucket(group_rank(cap,sector),range='0,1,0.1')"
    vol_group = "bucket(rank(ts_std_dev(ts_returns(close,1),20)),range = '0.1,1,0.1')"
    
    groups = ["market","sector", "industry", "subindustry", bps_group, cap_group, sector_cap_group]
    
    if region == "chn":
        groups += chn_group_13 + chn_group_1 + chn_group_2 + group_3 
    if region == "twn":
        groups += twn_group_13 + twn_group_1 + twn_group_2 + twn_group_8 
    if region == "asi":
        groups += asi_group_13 + asi_group_1 + asi_group_8 
    if region == "usa":
        groups += usa_group_13 + usa_group_1 + usa_group_2 #+ usa_group_3 + usa_group_4 + usa_group_8 + group_3 
        #groups += usa_group_5 + usa_group_6 + usa_group_7
    if region == "hkg":
        groups += hkg_group_13 + hkg_group_1 + hkg_group_2 + hkg_group_8
    if region == "kor":
        groups += kor_group_13 + kor_group_1 + kor_group_2 + kor_group_8
    if region == "eur": 
        groups += eur_group_13 + eur_group_1 + eur_group_2 + eur_group_3 + eur_group_8 +  eur_group_7 + group_3 
    if region == "glb":
        groups += glb_group_13 + glb_group_8 + glb_group_3 + glb_group_1 + glb_group_7 + group_3
    if region == "amr":
        groups += amr_group_3 + amr_group_13
    if region == "jpn":
        groups += jpn_group_1 + jpn_group_2 + jpn_group_13 + jpn_group_8
        
    for group in groups:
        if op.startswith("group_vector"):
            for vector in vectors:
                alpha = "%s(%s,%s,densify(%s))"%(op, field, vector, group)
                output.append(alpha)
        elif op.startswith("group_percentage"):
            alpha = "%s(%s,densify(%s),percentage=0.5)"%(op, field, group)
            output.append(alpha)
        else:
            alpha = "%s(%s,densify(%s))"%(op, field, group)
            output.append(alpha)
        
    return output