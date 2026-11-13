#!/usr/bin/env bash
# Optional native qualification runtime. Downloads only the exact official binaries.
set -euo pipefail
mkdir -p .tools
python - <<'PY'
import hashlib, pathlib, subprocess, tarfile
root=pathlib.Path('.tools')
packages=[
 ('kafka.tgz','https://archive.apache.org/dist/kafka/3.9.2/kafka_2.13-3.9.2.tgz',
  'sha512','01186a5086b5ae753811ee71808a797baec1303d4e0d4b1c0acf42c489654a7b8988136f8469d0b66f0be439f8e3e9b816171ea0b0d7a36462b07aa2d3ef0023'),
 ('jdk17.tgz','https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.21%2B9/OpenJDK17U-jdk_x64_linux_hotspot_17.0.21_9.tar.gz',
  'sha256','8b9b611d917e3dfd8419d3566b5949226f69b052fed7e109c1368add1dfe8711')]
for name,url,algorithm,expected in packages:
 path=root/name
 if not path.exists():
  subprocess.run(['curl','--fail','--location','--retry','2','--max-time','180',url,'-o',str(path)],check=True)
 observed=hashlib.new(algorithm,path.read_bytes()).hexdigest()
 if observed!=expected: raise RuntimeError(f'checksum mismatch: {name}')
 destination=root/('jdk17' if name=='jdk17.tgz' else 'kafka_2.13-3.9.2')
 if destination.exists(): continue
 if name=='jdk17.tgz':
  destination.mkdir()
  subprocess.run(['tar','-xzf',str(path),'-C',str(destination),'--strip-components=1'],check=True)
 else:
  subprocess.run(['tar','-xzf',str(path),'-C',str(root)],check=True)
 print(name,algorithm,observed)
PY
