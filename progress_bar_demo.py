# # from alive_progress import alive_bar
# # import time
# # for total in 5000,7000,4000, 0:
# #     with alive_bar(total) as bar:
# #         for _ in range(total):
# #             time.sleep(0.001)
# #             bar()

# from tqdm import tqdm
# from time import sleep

# text = ""
# for char in tqdm(["a", "b", "c", "d"], desc="Progress", leave=True):
#   sleep(0.25)  
#   text = text + char


# from tqdm import tqdm
# import time


# for i in tqdm(range(100), bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} {postfix[0]}', postfix=["ETA: ?"]):
#     time.sleep(0.1)
#     # update the ETA
#     remaining = (100 - i - 1) * 0.1
#     tqdm.set_postfix_str(f"ETA: {remaining:.2f} seconds", refresh=True)


# from tqdm import tqdm
# import time

# pbar = tqdm(total=100, bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} {postfix}')
# for i in range(100):
#     time.sleep(0.1)
#     # update the ETA
#     remaining = (100 - i - 1) * 0.1
#     pbar.set_postfix_str(f"ETA: {remaining:.2f} seconds", refresh=True)
#     pbar.update()
# pbar.close()



from tqdm import tqdm
import time

pbar = tqdm(total=1000, 
            bar_format='{l_bar}{bar:20}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]')
for i in range(1000):
    time.sleep(0.1)
    remaining = (1000 - i - 1) * 0.1
    # pbar.set_postfix_str(f"ETA: {remaining:.2f} seconds", refresh=True)
    pbar.update()
pbar.close()
