from __future__ import print_function
from datetime import datetime

import sys
import os
import matplotlib.pyplot as plt
import pandas as pd

"""
- time - time is Unix time (in milliseconds)
- message identifier
- sender
- recipients - pipe-separated list of email recipients
- topic - always empty
- mode - always "email"
"""


def pre_processing(raw_df):
    raw_df = raw_df.copy()
    raw_df.columns = ["time", "msg_id", "sender", "recipients", "topic", "mode"]
    raw_df['datetime'] = raw_df.time.apply(lambda x: datetime.fromtimestamp(x / 1000.0).strftime('%Y-%m-%d %H:%M:%S'))
    raw_df['date'] = raw_df.time.apply(lambda x: datetime.fromtimestamp(x / 1000.0).strftime('%Y-%m-%d'))
    raw_df.drop(columns=["topic", "mode"], inplace=True)
    raw_df.dropna(axis=0, inplace=True)
    raw_df = raw_df[["time", "date", "msg_id", "sender", "recipients"]]

    raw_df['sender'] = raw_df.sender.apply(lambda x: x.lower())
    sdr = raw_df.sender.value_counts().sort_values(ascending=False)
    senders = pd.DataFrame(columns=["person", "sent"])
    senders["person"] = sdr.index
    senders["sent"] = sdr.values

    rcp = raw_df.recipients.apply(lambda x: x.lower()).to_list()
    list_of_lists = list(map(lambda x: x.split('|'), rcp))
    flattened_rcp = [y for x in list_of_lists for y in x]
    df_rcp = pd.DataFrame(flattened_rcp, columns=["person"])

    rcv = df_rcp.person.value_counts().sort_values(ascending=False)
    recipients = pd.DataFrame(columns=["person", "received"])
    recipients["person"] = rcv.index
    recipients["received"] = rcv.values

    return raw_df, senders, recipients


def sel_make_ts(data_frame, rank_table, col, start, n):
    """
    df : input data frame
    rank_table : df with ['person', 'sent', 'received']
    col: column for descending sort (ex. 'sent', 'received')
    start : start ranking to review : 0 ~ length of "df"
    n : the number of persons to be selected
    """
    sender_list = rank_table.sort_values(by=col, ascending=False).person[start:start + n].to_list()
    # print("<top-{}-persons start from ranking {}> :\n{}".format(n, start, sender_list))

    df_sel = data_frame[data_frame.sender.isin(sender_list)].reset_index(drop=True)
    df_sel.drop(columns=["recipients", "msg_id", "date"], inplace=True)

    df_ts = df_sel.pivot_table(index='month', columns='sender', values='time', aggfunc='count')
    df_ts.fillna(0, inplace=True)
    df_ts = df_ts.astype(int)

    return df_ts, sender_list


def plot_ts(data_frame, target_4_counts, fig_suffix):
    """
    df : target data frame
    target_4_counts : target object for counting
    fig_suffix : suffix for title and figure name
    """
    plt.rcParams['figure.figsize'] = 18, 7
    data_frame.plot()
    plt.ylabel(target_4_counts + ' counts')
    plt.title(target_4_counts + ' counts by month for ' + fig_suffix)
    plt.savefig('./output/' + target_4_counts + '_trends_' + fig_suffix + '.png')


def unique_incoming_contact(data_frame, people_list):
    """
    df : reference data frame
    people_list : list of people
    """
    df_sel = data_frame[data_frame.recipients.isin(people_list)].reset_index(drop=True)
    df_sel.drop(columns=["time", "msg_id", "date"], inplace=True)

    df_ts = df_sel.pivot_table(index='month', columns='recipients', values='sender', aggfunc=lambda x: x.nunique())
    df_ts.fillna(0, inplace=True)
    df_ts = df_ts.astype(int)

    return df_ts


def main(raw_df):
    cleaned_df, senders, recipients = pre_processing(raw_df)
    cleaned_df['month'] = cleaned_df.time.apply(lambda x: datetime.fromtimestamp(x / 1000.0).strftime('%Y-%m'))

    # outputs #1
    """- csv file with three columns---"person", "sent", "received"---where the final two columns contain the number 
    of emails that person sent or received in the data set. This file should be sorted by the number of emails sent. """
    res_df = senders.merge(recipients, on='person', how="outer").sort_values(by=['sent', 'received'], ascending=False)
    res_df.fillna(0, inplace=True)
    res_df.to_csv('./output/email_status.csv', index=False)

    # outputs #2
    """- PNG image visualizing the number of emails sent over time by some of the most prolific senders in (1). - 
    There are no specific guidelines regarding the format and specific content of the visualization - you can choose 
    which and how many senders to include, and the type of plot - but you should strive to make it as clear and 
    informative as possible, making sure to represent time in some meaningful way. """
    df_ts_top10, pp_list10 = sel_make_ts(cleaned_df, res_df, 'sent', 0, 10)
    df_ts_next10, pp_list10_20 = sel_make_ts(cleaned_df, res_df, 'sent', 10, 10)
    plot_ts(df_ts_top10, 'E-mail', 'top10persons')
    plot_ts(df_ts_next10, 'E-mail', 'next_top10persons')

    # output #3
    """- visualization that shows, for the same people, the number of unique people/email addresses who contacted 
    them over the same time period. - The raw number of unique incoming contacts is not quite as important as the 
    relative numbers (compared across the individuals from (2) ) and how they change over time. """
    df_ts_rcv10 = unique_incoming_contact(cleaned_df, pp_list10)
    plot_ts(df_ts_rcv10, 'unique_incoming_contacts', 'top10persons')
    df_ts_rcv10_20 = unique_incoming_contact(cleaned_df, pp_list10_20)
    plot_ts(df_ts_rcv10_20, 'unique_incoming_contacts', 'next_top10persons')


if __name__ == "__main__":
    os.makedirs("./output", exist_ok=True)
    file = sys.argv[1]
    if file == "enron-event-history-all.csv":
        df = pd.read_csv(file, header=None, low_memory=False)
        main(df)
    else:
        print("{} is not correct input file".format(file))
