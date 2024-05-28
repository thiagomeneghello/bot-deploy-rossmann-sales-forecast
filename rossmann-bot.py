import pandas as pd
import json
import requests
from flask import Flask, Response, request

#constants
TOKEN ='7366559089:AAHinD-nKW-vH1OsvWwmNTtqyA5hLL6pWf8'
#Info about BOT
#https://api.telegram.org/bot7366559089:AAHinD-nKW-vH1OsvWwmNTtqyA5hLL6pWf8/getMe

# Bot get updates- mensagem enviadas pro bot
#https://api.telegram.org/bot7366559089:AAHinD-nKW-vH1OsvWwmNTtqyA5hLL6pWf8/getUpdates

# webhook
#https://api.telegram.org/bot7366559089:AAHinD-nKW-vH1OsvWwmNTtqyA5hLL6pWf8/setWebhook?url=https://68c7de2dd94151.lhr.life

# Bot send msg- mensagem enviadas pro bot
#https://api.telegram.org/bot7366559089:AAHinD-nKW-vH1OsvWwmNTtqyA5hLL6pWf8/sendMessage?chat_id=7270631089&text=Hi Thiago, Im doing good.
    
def send_message( chat_id, text ):
    url = 'https://api.telegram.org/bot{}/'.format(TOKEN)
    url = url + 'sendMessage?chat_id={}'.format( chat_id )
    r = requests.post( url, json={'text': text} )
    print('Status-code: {}'.format(r.status_code))
    return None
     
    


def load_dataset(store_id):
    #loading test dataset + merge dataset store
    df10 = pd.read_csv('test.csv')
    df_store_raw = pd.read_csv('store.csv', low_memory=False)
    df_test = pd.merge(df10, df_store_raw, how='left', on='Store')
    #choose store for prediction
    df_test = df_test[df_test['Store'] == store_id]

    if not df_test.empty:
        #remove closed days
        df_test = df_test[~df_test['Open'].isnull()]
        df_test = df_test[df_test['Open'] != 0]
        df_test = df_test.drop(['Id'], axis=1)
        # convert dataframe to json
        data = json.dumps( df_test.to_dict(orient='records'))
    else:
        data = 'erro'

    return data

def predict( data ):
    # calling API
    url = 'https://deploy-rossmann-sales-forecast.onrender.com/rossmann/predict'
    header = {'Content-type': 'application/json'}
    data = data

    r = requests.post( url, data=data, headers=header )
    print('Status-code: {}'.format(r.status_code))
    d1 = pd.DataFrame(r.json(), columns=r.json()[0].keys())

    return d1


def parse_message(message):
    chat_id = message['message']['chat']['id']
    store_id = message['message']['text']
    store_id = store_id.replace('/', '')
    try:
        store_id = int(store_id)
        
    except ValueError:
        store_id = 'error'
    
    return chat_id, store_id
    
#API Initialize
app = Flask(__name__)

@app.route( '/', methods=['POST', 'GET'])
def index ():
    if request.method == 'POST':
        message = request.get_json()
        chat_id, store_id = parse_message (message)

        if store_id != 'error':
            #loading data
            data = load_dataset(store_id)
            
            if data != 'error':
                #prediction
                d1 = predict( data )
                #calculation
                d2 = d1.loc[:, ['store', 'predictions']].groupby('store').sum().reset_index()
                #message
                msg = 'Store n. {} tem previsão de R$ {:,.2f} em vendas nas próximas 6 semanas'.format(
                    d2.loc['store'].values[0],
                    d2.loc['predictions'].values[0])
                
                send_message ( chat_id, msg)
                return Response('OK', status=200)
                       
            else:
                send_message ( chat_id, 'Store ID not avaiable')
                return Response('OK', status=200)
            
        else:
            send_message ( chat_id, 'Store ID is wrong')
            return Response('OK', status=200)

    else:
        return '<h1> Rossmann Bot</h1>'
            
if __name__ == '__main__':
    port = os.environ.get('PORT', 5000)
    app.run(host = '0.0.0.0', port = port)

