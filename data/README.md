# Dataset

**Enron-Spam** (Metsis, Androutsopoulos & Paliouras, 2006): 33,716 labelled emails
(17,171 spam, 16,545 ham), as a CSV packaged at
<https://github.com/MWiechmann/enron_spam_data>.

`scripts/setup-node.sh master` downloads `enron_spam_data.zip` here and runs
`prepare_data.py`, which writes `emails.jsonl` (one JSON email per line:
`id, subject, body, label` with 1 = spam, 0 = ham). That file is uploaded to HDFS
by `scripts/init-cluster.sh`.

Both files are git-ignored (51 MB). After cleaning and de-duplication in Spark,
about 28,900 unique emails remain.
