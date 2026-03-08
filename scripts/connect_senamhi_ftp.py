#!/usr/bin/env python3
"""
Conectar a ftp.senamhi.gob.pe y listar/descargar archivos.

Uso rápido:
  - Listar:  python scripts/connect_senamhi_ftp.py --list
  - Descargar: python scripts/connect_senamhi_ftp.py --get remote/path/file --out localfile

Credenciales:
  - Se buscan en las variables de entorno SENAMHI_USER y SENAMHI_PASS
  - O se pueden pasar con --user y --pass (no recomendado en entornos públicos)

"""
import os
import argparse
from ftplib import FTP, error_perm


def connect(host, user, passwd, passive=True):
    ftp = FTP(host, timeout=30)
    ftp.connect()
    ftp.login(user, passwd)
    ftp.set_pasv(passive)
    return ftp


def list_dir(ftp, path=""):
    try:
        items = ftp.nlst(path)
    except error_perm as e:
        # some servers return permission error for empty lists
        print('Error listing:', e)
        return []
    return items


def download_file(ftp, remote_path, local_path):
    with open(local_path, 'wb') as f:
        def callback(data):
            f.write(data)
        ftp.retrbinary(f'RETR {remote_path}', callback)


def main():
    p = argparse.ArgumentParser(description='Conectar a SENAMHI FTP')
    p.add_argument('--host', default='ftp.senamhi.gob.pe', help='FTP host')
    p.add_argument('--user', help='Usuario FTP')
    p.add_argument('--pass', dest='passwd', help='Contraseña FTP')
    p.add_argument('--list', action='store_true', help='Listar directorio raíz')
    p.add_argument('--ls', help='Listar directorio/archivo específico')
    p.add_argument('--get', help='Ruta remota a descargar (RETR)')
    p.add_argument('--out', help='Archivo local de salida para --get')
    p.add_argument('--passive', action='store_true', help='Forzar modo pasivo')
    args = p.parse_args()

    user = args.user or os.environ.get('SENAMHI_USER')
    passwd = args.passwd or os.environ.get('SENAMHI_PASS')

    if not user:
        user = input('Usuario FTP: ').strip()
    if not passwd:
        import getpass
        passwd = getpass.getpass('Contraseña FTP: ')

    print('Conectando a', args.host)
    ftp = connect(args.host, user, passwd, passive=args.passive)
    try:
        if args.list:
            items = list_dir(ftp, '')
            print('Listado raíz:')
            for it in items:
                print('-', it)
        if args.ls:
            items = list_dir(ftp, args.ls)
            print(f'Listado {args.ls}:')
            for it in items:
                print('-', it)
        if args.get:
            if not args.out:
                p.error('--out es obligatorio para --get')
            print(f'Descargando {args.get} -> {args.out}')
            download_file(ftp, args.get, args.out)
            print('Descarga completa')
    finally:
        try:
            ftp.quit()
        except Exception:
            pass


if __name__ == '__main__':
    main()
