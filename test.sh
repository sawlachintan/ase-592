
list1=('256m' '512m' '768m' '1024m' '1280m' '1536m' '1792m' '2048m' '2304m' '2560m' '2816m' '3072m' '3328m' '3584m' '3840m' '4096m')
list2=(1 2 3 4 5 6 7 8)

val=1
for ((i = 1; i < ${#list1[@]}; i += 2)); do
    for j in "${list2[@]}"; do
        for ((k = 1; k < ${#list1[@]}; k += 2)); do
            for l in "${list2[@]}"; do
                echo "Iteration ${val}"
                python3 word_count.py "${list1[i]}" "$j" "${list1[k]}" "$l" sample_data/medium_text.txt
                ((val++))
            done
        done
    done
done
