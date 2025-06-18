# D1LH-PLAT-DBX-GLOBAL-Artifacts
This repo would contain Databricks artifacts that can be referenced/reused by Data.One.Lakehouse. The instructions to import the repository in other repo are extensible explain here: 

In Central Databricks Artifact repository (https://github.com/bayer-int/D1LH-PLAT-DBX-Artifacts), hereby called child repo in this context, there is a folder called global_utils that replicates artifacts which are in the Global Repo (https://github.com/bayer-int/D1LH-PLAT-DBX-GLOBAL-Artifacts). In order to bring changes from Global Repo, there is a command in Git called subtree that is used to copy these files. A subtree is a feature that allows you to insert the history of one repository into another repository as a subdirectory. This can be useful for incorporating external libraries, frameworks, or components into a project while keeping their history separate. In this command, subtree-directory is the directory within your repository where the content from the remote repository will be added. This command should be executed in Git Terminal or in Visual Studio Terminal pointing to the repo which we want to include these changes.

Sample command: "git subtree add --prefix global_lib https://github.com/bayer-int/D1LH-PLAT-DBX-GLOBAL-Artifacts  master --squash"

![image](https://github.com/user-attachments/assets/30eca6a6-802f-4188-af3b-753ebd9d23bc)

### New Modules Headers and Newsletter

Every time a new module is added should be added in the global_utils folder in order to have the newsletter page updated, also it must contain a header informing about this fields: 

""" <br>
Module: Module Title <br>
Purpose: This is a test for new modules <br>
Version: 1.0 <br>
Author: Athour Name <br>
Owner: Owner Name <br>
Email: Contact Mail <br>
Dependencies: if the module need to import some libraries or it has other dependencies <br>
Usage: How it works or how can be used <br>
Reviewers: If there is some codeowner <br>
History: <br>
    Date: 2023-01-01, Version: 1.0  Author: <Author name>, Description Changing things <br>
"""

In addition, in case there are some functions it should included also a header explaining some basic things due to in the same module several developers could work.

  """ <br>
  Function: Function Title <br>
  Description: Description of the function and what it does <br>
  Parameters: We should explain the parameters that the function needs to work <br>
     - storage_account (str): Name of the ADLS account. <br>
     - container (str): Name of the discovery container. <br>
  Returns: What value will return  str - String representing the database name in hive. <br>
  Author: Function author <br>
  Date: 2023-01-01 <br>
  """

Regarding the newsletter, in order to be updated of the last modules that are being included in the repo, you will need to add your email to this json file inside the global repo: newsletter_subscribers/subscribers.json, also in the confluence there will be updates everytime a new module has been added. Here the structure of the newsletter workload: 
![image](https://github.com/user-attachments/assets/35627ebb-c3e9-4d70-a011-237ab681f830)



### Documentation
https://confluence.cloud.bayer.com/spaces/EGDE/pages/579372041/Databricks+Global+Artifacts+Repo
https://confluence.cloud.bayer.com/spaces/EGDE/pages/608705817/Global+Artifact+NewsLetter

