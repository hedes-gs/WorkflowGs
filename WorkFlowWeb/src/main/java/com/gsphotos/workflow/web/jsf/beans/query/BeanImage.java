package com.gsphotos.workflow.web.jsf.beans.query;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.faces.context.FacesContext;
import javax.inject.Inject;
import javax.inject.Named;

import org.primefaces.model.DefaultStreamedContent;
import org.primefaces.model.StreamedContent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;

import com.gsphotos.workflow.dao.ImageFilterDAO;
import com.workflow.model.HbaseImageThumbnail;

@Named
@Scope("view")
public class BeanImage {

	protected List<HbaseImageThumbnail> listOfHbaseImageThumbnail;
	protected List<HbaseImageThumbnail> listOfFullWidthHbaseImageThumbnail;

	@Autowired
	@Inject
	protected ImageFilterDAO imgFilterDAO;

	protected HbaseImageThumbnail selectedImg;

	@PostConstruct
	public void init() {
		listOfHbaseImageThumbnail = imgFilterDAO.getThumbNailsByDate(LocalDateTime.now().minusDays(50),
				LocalDateTime.now().plusDays(2), 0, 0, HbaseImageThumbnail.class).stream().filter((h) -> {
					return h.isOrignal() && h.getWidth() < 200;
				}).collect(Collectors.toList());
		listOfFullWidthHbaseImageThumbnail = imgFilterDAO.getThumbNailsByDate(LocalDateTime.now().minusDays(50),
				LocalDateTime.now().plusDays(2), 0, 0, HbaseImageThumbnail.class).stream().filter((h) -> {
					return h.isOrignal() && h.getWidth() > 200;
				}).collect(Collectors.toList());
	}

	public StreamedContent getImage() throws IOException {
		FacesContext context = FacesContext.getCurrentInstance();

		// So, browser is requesting the image. Return a real StreamedContent with the
		// image bytes.
		String thumbId = context.getExternalContext().getRequestParameterMap().get("thumbId");
		for (HbaseImageThumbnail hit : listOfHbaseImageThumbnail) {
			if (hit.getImageId().equals(thumbId)) {
				return new DefaultStreamedContent(new ByteArrayInputStream(hit.getThumbnail()), "image/jpg");
			}
		}

		return new DefaultStreamedContent();
	}

	public List<HbaseImageThumbnail> getThumbs() {
		return listOfHbaseImageThumbnail;
	}

	public HbaseImageThumbnail getSelectedImg() {
		return selectedImg;
	}

	public void setSelectedImg(HbaseImageThumbnail selectedImg) {
		Optional<HbaseImageThumbnail> value = listOfFullWidthHbaseImageThumbnail.stream().filter((h) -> {
			return h.getImageId().equals(selectedImg.getImageId());
		}).findFirst();
		this.selectedImg = value.orElse(selectedImg);
	}

}
