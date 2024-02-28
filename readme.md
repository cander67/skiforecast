# Ski Weather Outlook
Lift tickets are expensive. Ski area parking lots fill up. Day pass limits... There are plenty of reasons to choose wisely when planning a ski weekend. This app helps you plan by providing a dashboard view of NOAA weather forecasts for popular ski areas located in the Cascade Mountains of Washington State. The app updates once per day and summarizes precipitation, snow levels, temperature and wind data for each day of the upcoming week.

Access the [Ski Weather Outlook](https://skiforecast.z5.web.core.windows.net/) app.

Check out how it works [here](https://skiforecast.z5.web.core.windows.net/pages/doc.html).

### Version 1.0.0 (Released: February 24, 2024)

## Getting Started
Clone the repository from the terminal using the following command: `git clone https://github.com/cander67/skiforecast.git`

### Prerequisites
Local development can be accomplished by creating a virtual environment using Python 3.11 and installing the necessary dependencies as described in the next section.

The following prerequisites must be met for cloud deployment:
- Azure Developer CLI
- Azure Function App
- Azure Function App Registration and Service Principal
- Azure Storage Account, standard general-purpose v2 account
- Microsoft Entra security group
- Azure RBAC assignments
- Properly configured environment variables
    - `AZURE_CLIENT_ID`
    - `AZURE_TENANT_ID`
    - `AZURE_CLIENT_SECRET`
    - User-Agent Header for API requests

### Installing
- Install [Azure Developer CLI](https://learn.microsoft.com/en-us/azure/developer/azure-developer-cli/overview)
- Install dependencies from the terminal using the following command: `pip install -r requirements.txt`

## Deployment
This app is setup for [continous deployment](https://learn.microsoft.com/en-us/azure/azure-functions/functions-continuous-deployment) to [Azure Functions](https://azure.microsoft.com/en-us/products/functions) using [GitHub Actions](https://docs.github.com/en/actions).

## Built With
- [Azure Functions](https://azure.microsoft.com/en-us/products/functions)
- [Azure Static Website Hosing](https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blob-static-website)

## Contributing
Contributions and suggestions are welcome and may be submitted by forking this project and submitting a pull request through GitHub.

## Changelog

### Version 1.0.0 (Released: February 24, 2024)
- Initial release

## Authors
Written by Cyrus Anderson.

## License
This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledements
Thanks to varunr89 for brainstorming and motivation.