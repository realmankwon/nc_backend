# nextcolony

## Install pyenv

Download

```
curl https://pyenv.run | bash
```

Setting path

```
$ vi ~/.bashrc

export PATH="$HOME/.pyenv/bin:$PATH"
eval "$(pyenv init --path)"
eval "$(pyenv init -)"
eval "$(pyenv virtualenv-init -)"
```

Setting shell

```
source ~/.bashrc  # or source ~/.zshrc
```

```
pyenv install 3.12.0
pyenv virtualenv 3.12.0 nextcolony
pyenv activate nextcolony
python -m pip install -r requirements.txt
```

## Install

```
apt-get install build-essential libssl-dev python3-dev git python3-pip
```

Server

```
apt install mysql-server libmysqlclient-dev
```

```
mysql_secure_installation
```

```
apt install phpmyadmin php-mbstring php-gettext
```

## Setup

```
pip3 install wheel beem dataset mysqlclient base36
```

create steembattle database and tables based on /sql

create new file config.json:

```
{
        "databaseConnector": "mysql://db_user:db_password@localhost/db_name?charset=utf8mb4",
        "custom_json_id": "nextcolony",
        "transfer_id": "nc",
        "stop_block_num": -1

}
```

### python libs

```
sudo pip3 install unidecode
```
