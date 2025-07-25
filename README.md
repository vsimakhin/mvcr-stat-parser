**English**: Generates charts based on the official MVCR stats (https://mv.gov.cz/clanek/cizinci-s-povolenym-pobytem.aspx) for foreigners in the Czech Republic

**Česky**: Generuje grafy z oficiálních statistik MVČR o cizincích s povoleným pobytem v ČR

![image](https://github.com/user-attachments/assets/3fa6a8b9-d5df-43a0-b6bf-e9b95021d200)

### Usage 

You need python installed. Clone or download the repository from github and run:

```bash
pipenv install
pipenv shell
python app.py
```


### Data update

- Download new files from https://mv.gov.cz/clanek/cizinci-s-povolenym-pobytem.aspx to the `source` folder
- run `python ./parser.py`
