import requests
import json
import yaml
import sys
sys.path.append("./")
class Chats:
    def __init__(self, chat_space_name):
        self.url = self._get_chat_space_url(chat_space_name)

    def _get_chat_space_url(self,chat_space_name):
        try:
            with open('config/google_chat/google_chat_config.yaml', 'r') as stream:
                data = yaml.safe_load(stream)
            spaces= data.get('chat_spaces')
            for space in spaces:
                if space['name'] == chat_space_name:
                    return space['url']    
        except Exception as e:
            print(f"Error in load chat-config-yml file: {e}")
    def post_message(self, payload):
        response = requests.post(self.url, data=payload)
        return response.text
    def seconds_to_hms(self,d):
        d = int(d)  
        h = d // 3600
        m = (d % 3600) // 60
        s = d % 60

        h_display = "{:d} hour, ".format(h) if h == 1 else "{:d} hours, ".format(h) if h > 0 else ""
        m_display = "{:d} minute, ".format(m) if m == 1 else "{:d} minutes, ".format(m) if m > 0 else ""
        s_display = "{:d} second".format(s) if s == 1 else "{:d} seconds".format(s) if s > 0 else ""
        return h_display + m_display + s_display
    
        
    def workflow_end_message(self,workflow_name,os_name,stated_at,ended_at,duration,row_processed,result_url):
        if os_name=='Windows':
            imageUrl='https://i.imgur.com/jolpd9S.png'
        else:
            imageUrl='https://i.imgur.com/apD5z9k.png'
        payload = json.dumps(
            {
                "cardsV2": [
                    {
                        "cardId": "unique-card-id",
                        "card": {
                            "header": {
                                "title": f"{workflow_name}",
                                "subtitle": f"Workflow Finished Running",
                                "imageUrl": f"{imageUrl}",
                                "imageType": "SQUARE",
                            },
                            "sections": [
                                {
                                    "header": "Informations: ",
                                    "collapsible": False,
                                    "widgets": [
                                        {
                                            "textParagraph": {
                                                "text": f"<b> Started At: </b>{stated_at} "
                                            }
                                        },
                                        {
                                            "textParagraph": {
                                                "text": f"<b> Ended At: </b> {ended_at}"
                                            }
                                        },
                                        {
                                            "textParagraph": {
                                                "text": f"<b> Time run:</b> {self.seconds_to_hms(duration)}"
                                            }
                                        },
                                        {
                                            "textParagraph": {
                                                "text": f"<b> Row processed: </b>{row_processed}"
                                            }
                                        },
                                        {"divider": {}},
                                        {
                                            "buttonList": {
                                                "buttons": [
                                                    {
                                                        "text": "Results",
                                                        "color": {
                                                            "red": 0,
                                                            "green": 0.5,
                                                            "blue": 1,
                                                            "alpha": 1,
                                                        },
                                                        "onClick": {
                                                            "openLink": {
                                                                "url": f"{result_url}"
                                                            }
                                                        },
                                                    },
                                                ]
                                            }
                                        },
                                    ],
                                }
                            ],
                        },
                    }
                ]
            }
        )
        print("acitivities message send to google chat")
        self.post_message(payload)
    
