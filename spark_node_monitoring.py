import subprocess

def get_worker_details():
  def get_hostname_and_space():
    hostname = subprocess.check_output("hostname",shell=True).decode().strip()

    df_output = subprocess.check_output("df /local_disk0/",shell=True).decode().split()
    space_in_kb = int(df_output[10])
    space_in_gb = "{:.2f}".format(space_in_kb/1024/1024)

    mem_output = subprocess.check_output("free - m", shell=True).decode().split('\n')
    mem_info = mem_output[1].split()
    total_memory_gb = "{:.2f}".format(int(mem_info[1])/1024)
    used_memory_gb = "{:.2f}".format(int(mem_info[2])/1024)
    free_memory_gb = "{:.2f}".format(int(mem_info[3])/1024)

    ip_address = subprocess.check_output("hostname - I", shell=True).decode().stript()

    return (ip_address, hostname, space_in_gb, total_memory_gb, used_memory_gb, free_memory_gb)

  df = sc.prallelize(range(200)).map(lambda _:get_hostname_and_space()).distinct().toDF()\
      .selectExpr(
          "_1 as ip_address",
          "_2 as hostname",
          "_3 as space_in_gb",
          "_4 as total_memory_gb",
          "_5 as used_memory_gb",
          "_6 as free_memory_gb",
          "current_timestamp() as loaded_date_time"
      )

  return df

get_worker_details().display()


  
