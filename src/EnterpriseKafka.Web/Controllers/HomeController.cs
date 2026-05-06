using Microsoft.AspNetCore.Mvc;

namespace EnterpriseKafka.Web.Controllers;

public class HomeController : Controller
{
    public IActionResult Index() => View();
}
