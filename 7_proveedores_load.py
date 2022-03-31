import json
import aiohttp
import asyncio
import time
import os
from tqdm import tqdm

def print_time(seconds, message):
    seconds = int(seconds)
    horas = seconds//3600
    minutos = seconds//60 - (horas*60)
    segundos = seconds - horas*3600 - minutos*60
    print(u"Takes: {} hours, {} minutes, {} seconds {}".format(horas, minutos, segundos, message))

async def get_info(session, url, ruc, rucs):
    try: 
        async with session.get(url) as resp:
            item = await resp.json(content_type=None)
            if resp.status == 200:
                #print(item['proveedor']['numeroRuc'])
                return item
            else:
                #print(u'Error 1 {}'.format(ruc))
                pass
                #return {"results": f"timeout error on {url}"}
    except:
        #print(u"Timeout error on {url}".format(url))
        print(u'Error Tipo 1 {}'.format(ruc))
        time.sleep(1)
        try: 
            async with session.get(url) as resp:
                item = await resp.json(content_type=None)
                if resp.status == 200:
                    print(u'Error Tipo 1 arreglado {}'.format(item['proveedor']['numeroRuc']))
                    return item
                else:
                    #print(u'Error 3 {}'.format(ruc))
                    pass
        except:
            print(u'Error tipo 2 {}'.format(ruc))
            time.sleep(1)
            try: 
                async with session.get(url) as resp:
                    item = await resp.json(content_type=None)
                    if resp.status == 200:
                        print(u'Error Tipo 2 arreglado {}'.format(item['proveedor']['numeroRuc']))
                        return item
                    else:
                        #print(u'Error 5 {}'.format(ruc))
                        pass
            except:
                print(u'Error Tipo 3 Fatal {}'.format(ruc))
                #return {"results": f"timeout error on {url}"}
        #return {"results": f"timeout error on {url}"}

def get_a_b_limits(rucs):
    step = 20
    n_complete = len(rucs)//step
    a = [i*step for i in range(n_complete + 1)]
    b = [(i+1)*step for i in range(n_complete + 1)]
    b[-1] = len(rucs)
    return a, b

async def main(a,b):
    async with aiohttp.ClientSession() as session:
        #with open('rucs_proveedores.json', 'r', encoding='utf-8') as f:
        with open('rucs_proveedores_2021_2022.json', 'r', encoding='utf-8') as f:
            rucs = json.load(f, encoding='utf-8')
        tasks = []
        for ruc in list(rucs.keys())[a:b]:
            #print(ruc)
            url = f'https://proveedores-del-estado-api-jc2kvxncma-uc.a.run.app/info_proveedores?ruc={ruc}' 
            tasks.append(asyncio.ensure_future(get_info(session, url, ruc, rucs)))

        total = await asyncio.gather(*tasks)
        return total

def save_data(data):
    output_file = open(os.getcwd() + '/proveedores/proveedores_load.txt', 'w', encoding='utf-8')
    for dic in data:
        json.dump(dic, output_file) 
        output_file.write("\n")

if __name__ == '__main__':
    print("********************         LOADING PROVEEDORES        ********************\n\n")
    print("Loading..")
    t0 = time.time()
    original = []
    #with open('rucs_proveedores.json', 'r', encoding='utf-8') as f:
    with open('rucs_proveedores_2021_2022.json', 'r', encoding='utf-8') as f:
        rucs = json.load(f, encoding='utf-8')
    a,b = get_a_b_limits(rucs)
    #print(a)
    #print(b)
    for i in tqdm(range(len(a))):
        #print(u'Range [{} - {}]'.format(a[i], b[i]))
        original_ = asyncio.run(main(a[i], b[i]))
        time.sleep(2)
        original += original_
        if i % 5 == 0:
            save_data(original)
    print(len(original))
    save_data(original)
    t1 = time.time()
    print_time(t1 - t0, 'download using async.')
    print("********************")