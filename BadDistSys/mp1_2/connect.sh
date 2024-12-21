# #!/bin/bash 
 
# # List of servers (replace these with your actual server IPs or hostnames) 
# servers=( 
#     # ================================================== STOP ==================================================  
#     # "fa24-cs425-1501.cs.illinois.edu" // add this at ur own risk, it will rm -rf baddistsys 
#     # ================================================== STOP ==================================================  
#     "fa24-cs425-1502.cs.illinois.edu" 
#     "fa24-cs425-1503.cs.illinois.edu" 
#     "fa24-cs425-1504.cs.illinois.edu" 
#     "fa24-cs425-1505.cs.illinois.edu" 
#     "fa24-cs425-1506.cs.illinois.edu" 
#     "fa24-cs425-1507.cs.illinois.edu" 
#     "fa24-cs425-1508.cs.illinois.edu" 
#     "fa24-cs425-1509.cs.illinois.edu" 
#     "fa24-cs425-1510.cs.illinois.edu" 
# ) 
 
# # Name of the tmux session 
# SESSION_NAME="baddistsys"
# ENTER_REPO="cd baddistsys"
# PULL_CMD="git pull origin main"
# RUN_SERVER_CMD="go run server_main.go"
 
# # Create a new tmux session in detached mode 
# tmux new-session -d -s $SESSION_NAME 
 
# # Loop through the servers and open a new window for each 
# for i in "${!servers[@]}"; do 
#     if [ "$i" -eq 0 ]; then 
 
#         # scp file over 
#         tmux send-keys -t $SESSION_NAME "scp -r ~/baddistsys/* ${servers[$i]}:/home/cs104/baddistsys/" C-m 
 
#         tmux send-keys -t $SESSION_NAME "ssh ${servers[$i]} -o StrictHostKeyChecking=no" C-m 
 
#         # remove baddistsys 
#         # tmux send-keys -t $SESSION_NAME "rm -rf baddistsys 2> /dev/null" C-m 
#         # clone git repo 
#         # tmux send-keys -t $SESSION_NAME "$CLONE_CMD" C-m 
 
#         # run server 
#         tmux send-keys -t $SESSION_NAME "$RUN_SERVER_CMD" C-m 
#     else 
#         # scp file over 
#         tmux new-window -t $SESSION_NAME "scp -r ~/baddistsys/* ${servers[$i]}:/home/cs104/baddistsys/ " C-m 
 
#         tmux send-keys -t $SESSION_NAME "ssh ${servers[$i]} -o StrictHostKeyChecking=no" 
         
#         # remove baddistsys 
#         # tmux send-keys -t $SESSION_NAME "rm -rf baddistsys 2> /dev/null" C-m 
#         # clone git repo 
#         # tmux send-keys -t $SESSION_NAME "$CLONE_CMD" C-m 
 
#         # run server 
#         tmux send-keys -t $SESSION_NAME "$RUN_SERVER_CMD" C-m 
#     fi 
# done 
 
# # Attach to the tmux session 
# tmux attach -t $SESSION_NAME
#!/bin/bash 
 
# List of servers (replace these with your actual server IPs or hostnames) 
#!/bin/bash 
 
# List of servers 
servers=( 
    "fa24-cs425-1502.cs.illinois.edu" 
    "fa24-cs425-1503.cs.illinois.edu" 
    "fa24-cs425-1504.cs.illinois.edu" 
    "fa24-cs425-1505.cs.illinois.edu" 
    "fa24-cs425-1506.cs.illinois.edu" 
    "fa24-cs425-1507.cs.illinois.edu" 
    "fa24-cs425-1508.cs.illinois.edu" 
    "fa24-cs425-1509.cs.illinois.edu" 
    "fa24-cs425-1510.cs.illinois.edu" 
) 
 
# Name of the tmux session 
SESSION_NAME="baddistsys"
 
# Create a new tmux session in detached mode 
tmux new-session -d -s $SESSION_NAME 
 
# Loop through the servers and open a new window for each 
for i in "${!servers[@]}"; do 
    # Create a new window and SSH into the server
    tmux new-window -t $SESSION_NAME -n "server-$i" "ssh ${servers[$i]} -o StrictHostKeyChecking=no; bash" 
done 
 
# Attach to the tmux session 
tmux attach -t $SESSION_NAME